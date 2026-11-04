package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"proxypool/internal/domain"
)

var ErrBindingVersionConflict = errors.New("sticky binding version conflict")

// Binding states: ACTIVE is reusable, CLAIMING marks a rebind fetch in
// progress (the claim prevents a duplicate paid vendor fetch for the same
// session), EXPIRED is claimable again.
const (
	BindingActive   = "ACTIVE"
	BindingClaiming = "CLAIMING"
	BindingExpired  = "EXPIRED"
)

// ClaimStickySession synchronizes the project+session slot before any paid
// fetch. It returns the current binding and what the caller should do:
//   - reused: an ACTIVE unexpired binding exists; deliver it, claim nothing.
//   - claimed: this caller owns the rebind slot (state CLAIMING) until
//     claimDeadline; it must finish with PutBinding or FailStickyClaim.
//   - both false: another request holds an unexpired claim; callers wait or
//     return a shortfall instead of fetching a second paid connection.
//
// An expired row (any state) is re-claimed by version CAS, and the claimed
// row's ciphertext is cleared so a stale plaintext snapshot can never be
// served while a rebind is in flight.
func (s *Store) ClaimStickySession(ctx context.Context, projectID, sessionID,
	owner string, claimDeadline, now time.Time) (binding domain.StickyBinding, reused, claimed bool, err error) {
	stamp := fmtTime(now)
	err = s.tx(ctx, func(t *sql.Tx) error {
		row := t.QueryRowContext(ctx, `
			SELECT version, fingerprint, source_id, source_config_version,
			       resource_id, incarnation, connection_ciphertext, validation_json,
			       deadline, state
			FROM sticky_bindings WHERE project_id = ? AND session_id = ?`,
			projectID, sessionID)
		var b domain.StickyBinding
		var validation, deadline string
		scanErr := row.Scan(&b.Version, &b.Fingerprint, &b.SourceID, &b.SourceConfigVersion,
			&b.ResourceID, &b.Incarnation, &b.ConnectionCiphertext, &validation,
			&deadline, &b.State)
		switch {
		case errors.Is(scanErr, sql.ErrNoRows):
			res, ierr := t.ExecContext(ctx, `
				INSERT INTO sticky_bindings(project_id, session_id, version, owner,
				  fingerprint, source_id, source_config_version, resource_id, incarnation,
				  connection_ciphertext,
				  validation_json, deadline, state, created_at, updated_at)
				VALUES(?,?,1,?,'','',0,'','','','{}',?,'CLAIMING',?,?)`,
				projectID, sessionID, owner, fmtTime(claimDeadline), stamp, stamp)
			if ierr != nil {
				return ierr
			}
			if n, ierr := res.RowsAffected(); ierr != nil {
				return ierr
			} else if n != 1 {
				return ErrBindingVersionConflict
			}
			binding = domain.StickyBinding{ProjectID: projectID, SessionID: sessionID,
				Version: 1, Owner: owner, State: BindingClaiming, Deadline: claimDeadline}
			claimed = true
			return nil
		case scanErr != nil:
			return scanErr
		}
		b.ProjectID = projectID
		b.SessionID = sessionID
		exp, perr := time.Parse(time.RFC3339Nano, deadline)
		if perr != nil {
			return perr
		}
		b.Deadline = exp
		if b.State == BindingActive && exp.After(now) {
			if jerr := json.Unmarshal([]byte(validation), &b.Validation); jerr != nil {
				return jerr
			}
			binding = b
			reused = true
			return nil
		}
		if b.State == BindingClaiming && exp.After(now) {
			// Someone else is fetching; the row stays untouched.
			binding = b
			return nil
		}
		res, uerr := t.ExecContext(ctx, `
			UPDATE sticky_bindings SET
			  version = version + 1,
			  owner = ?,
			  fingerprint = '',
			  source_id = '',
			  source_config_version = 0,
				  resource_id = '',
				  incarnation = '',
			  connection_ciphertext = '',
			  validation_json = '{}',
			  deadline = ?,
			  state = 'CLAIMING',
			  updated_at = ?
			WHERE project_id = ? AND session_id = ? AND version = ?`,
			owner, fmtTime(claimDeadline), stamp, projectID, sessionID, b.Version)
		if uerr != nil {
			return uerr
		}
		if n, uerr := res.RowsAffected(); uerr != nil {
			return uerr
		} else if n != 1 {
			// A concurrent claim won the CAS race; report busy.
			binding = b
			return nil
		}
		binding = domain.StickyBinding{ProjectID: projectID, SessionID: sessionID,
			Version: b.Version + 1, Owner: owner, State: BindingClaiming,
			Deadline: claimDeadline}
		claimed = true
		return nil
	})
	return binding, reused, claimed, mapErr(err)
}

// ClaimStickyRebind converts a still-live but currently unusable binding into
// this request's CLAIMING state. The version CAS prevents a stale eligibility
// decision from replacing a newer binding created by a concurrent request.
func (s *Store) ClaimStickyRebind(ctx context.Context, projectID, sessionID,
	owner string, expectedVersion int64, claimDeadline, now time.Time) (bool, error) {
	stamp := fmtTime(now)
	res, err := s.db.ExecContext(ctx, `
		UPDATE sticky_bindings SET
		  version = version + 1,
		  owner = ?,
		  fingerprint = '',
		  source_id = '',
		  source_config_version = 0,
		  resource_id = '',
		  incarnation = '',
		  connection_ciphertext = '',
		  validation_json = '{}',
		  deadline = ?,
		  state = 'CLAIMING',
		  updated_at = ?
		WHERE project_id = ? AND session_id = ? AND version = ?
		  AND state IN ('ACTIVE','EXPIRED')`,
		owner, fmtTime(claimDeadline), stamp, projectID, sessionID, expectedVersion)
	if err != nil {
		return false, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n == 1, mapErr(err)
}

// FailStickyClaim releases a claim after a failed rebind so the next request
// can fetch immediately instead of waiting out the claim deadline. Only the
// claim owner can fail it; a taken-over claim is left alone.
func (s *Store) FailStickyClaim(ctx context.Context, projectID, sessionID,
	owner string, expectedVersion int64, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE sticky_bindings SET state = 'EXPIRED', updated_at = ?
		WHERE project_id = ? AND session_id = ? AND owner = ? AND version = ?
		  AND state = 'CLAIMING'`,
		fmtTime(now), projectID, sessionID, owner, expectedVersion)
	return mapErr(err)
}

// PutBinding stores a sticky binding. expectedVersion 0 creates the binding
// and fails when one already exists; any other value must match the stored
// version, so a slow old writer can never overwrite a newer rebinding. A
// successful rebinding bumps the version.
func (s *Store) PutBinding(ctx context.Context, b domain.StickyBinding,
	expectedVersion int64, now time.Time) error {
	validationJSON := marshalJSONDefault(b.Validation)
	stamp := fmtTime(now)
	return s.tx(ctx, func(t *sql.Tx) error {
		if expectedVersion <= 0 {
			res, err := t.ExecContext(ctx, `
				INSERT INTO sticky_bindings(project_id, session_id, version, owner, fingerprint,
					  source_id, source_config_version, resource_id, incarnation,
					  connection_ciphertext, validation_json,
				  deadline, state, created_at, updated_at)
					VALUES(?,?,1,?,?,?,?,?,?,?,?,?, 'ACTIVE',?,?)
				ON CONFLICT(project_id, session_id) DO NOTHING`,
				b.ProjectID, b.SessionID, b.Owner, b.Fingerprint, b.SourceID,
				b.SourceConfigVersion, b.ResourceID, b.Incarnation, b.ConnectionCiphertext, validationJSON,
				fmtTime(b.Deadline), stamp, stamp)
			if err != nil {
				return err
			}
			if n, err := res.RowsAffected(); err == nil && n == 0 {
				return ErrBindingVersionConflict
			}
			return nil
		}
		res, err := t.ExecContext(ctx, `
			UPDATE sticky_bindings SET
			  version = version + 1,
			  owner = ?,
			  fingerprint = ?,
			  source_id = ?,
			  source_config_version = ?,
				  resource_id = ?,
				  incarnation = ?,
			  connection_ciphertext = ?,
			  validation_json = ?,
			  deadline = ?,
			  state = 'ACTIVE',
			  updated_at = ?
			WHERE project_id = ? AND session_id = ? AND version = ?`,
			b.Owner, b.Fingerprint, b.SourceID, b.SourceConfigVersion, b.ResourceID,
			b.Incarnation, b.ConnectionCiphertext,
			validationJSON, fmtTime(b.Deadline), stamp,
			b.ProjectID, b.SessionID, expectedVersion)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrBindingVersionConflict
		}
		return nil
	})
}

func (s *Store) GetBinding(ctx context.Context, projectID, sessionID string) (domain.StickyBinding, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT project_id, session_id, version, owner, fingerprint, source_id,
		       source_config_version, resource_id, incarnation, connection_ciphertext, validation_json,
		       deadline, state, created_at, updated_at
		FROM sticky_bindings WHERE project_id = ? AND session_id = ?`,
		projectID, sessionID)
	var b domain.StickyBinding
	var validation string
	var deadline, created, updated string
	err := row.Scan(&b.ProjectID, &b.SessionID, &b.Version, &b.Owner, &b.Fingerprint, &b.SourceID,
		&b.SourceConfigVersion, &b.ResourceID, &b.Incarnation, &b.ConnectionCiphertext, &validation,
		&deadline, &b.State, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.StickyBinding{}, domain.NotFound("sticky binding")
	}
	if err != nil {
		return domain.StickyBinding{}, mapErr(err)
	}
	if err := json.Unmarshal([]byte(validation), &b.Validation); err != nil {
		return domain.StickyBinding{}, err
	}
	if b.Deadline, err = time.Parse(time.RFC3339Nano, deadline); err != nil {
		return domain.StickyBinding{}, err
	}
	if b.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return domain.StickyBinding{}, err
	}
	if b.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return domain.StickyBinding{}, err
	}
	return b, nil
}

// ExpireBindings flips lapsed bindings to EXPIRED; the stored fingerprint and
// ciphertext stay so the delivery path can decide whether the session's CD
// exception still applies.
func (s *Store) ExpireBindings(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE sticky_bindings SET state = 'EXPIRED', updated_at = ?
		WHERE state = 'ACTIVE' AND deadline <= ?`, fmtTime(now), fmtTime(now))
	if err != nil {
		return 0, mapErr(err)
	}
	n, err := res.RowsAffected()
	return n, mapErr(err)
}

// DeleteBinding removes a released binding; the project CD window itself is
// never touched from here.
func (s *Store) DeleteBinding(ctx context.Context, projectID, sessionID string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM sticky_bindings WHERE project_id = ? AND session_id = ?`,
		projectID, sessionID)
	if err != nil {
		return mapErr(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return mapErr(err)
	}
	if n == 0 {
		return domain.NotFound("sticky binding")
	}
	return nil
}
