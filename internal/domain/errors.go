package domain

import "fmt"

type Error struct {
	Code    string
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func E(code string, status int, format string, args ...any) *Error {
	return &Error{Code: code, Status: status, Message: fmt.Sprintf(format, args...)}
}

func InvalidRequest(format string, args ...any) *Error {
	return E("INVALID_REQUEST", 400, format, args...)
}
func UnsupportedProfile(profile string) *Error {
	return E("UNSUPPORTED_PROFILE", 400, "validation profile %q is not supported", profile)
}
func UnsupportedTransport(t string) *Error {
	return E("UNSUPPORTED_TRANSPORT", 400, "proxy transport %q is not supported", t)
}
func Unauthenticated() *Error {
	return E("UNAUTHENTICATED", 401, "missing or invalid bearer token")
}
func NotFound(what string) *Error {
	return E("NOT_FOUND", 404, "%s not found", what)
}
func AcquireCapacity() *Error {
	return E("ACQUIRE_CAPACITY_EXHAUSTED", 503, "too many concurrent acquisition operations")
}
func NoEligibleCandidates(detail string) *Error {
	return E("NO_ELIGIBLE_CANDIDATES", 503, "no eligible candidates: %s", detail)
}
func InsufficientValidated(detail string) *Error {
	return E("INSUFFICIENT_VALIDATED_RESOURCES", 503, "%s", detail)
}
func ValidationInfra(detail string) *Error {
	return E("VALIDATION_INFRA_UNAVAILABLE", 503,
		"validation infrastructure unavailable, nothing delivered: %s", detail)
}
func StoreUnavailable(err error) *Error {
	return E("ALLOCATION_STORE_UNAVAILABLE", 503, "allocation store unavailable: %v", err)
}
func AsError(err error) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}
	return E("INTERNAL", 500, "internal error: %v", err)
}
