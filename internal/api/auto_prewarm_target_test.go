package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"proxypool/internal/storage/sqlite"
)

func TestProjectAutoPrewarmTargetCASAndBounds(t *testing.T) {
	e := newImportIdentityServices(t)
	ctx := context.Background()
	for _, target := range []int{100, 0, -1, 1001} {
		before, err := e.store.GetProject(ctx, sqlite.DefaultProjectID)
		if err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf(`{"config_version":%d,"auto_prewarm_target":%d}`, before.ConfigVersion, target)
		r := httptest.NewRequest(http.MethodPatch, "/api/v1/admin/projects/default", strings.NewReader(body))
		r.SetPathValue("id", sqlite.DefaultProjectID)
		w := httptest.NewRecorder()
		e.server.handlePatchProject(w, r)
		after, err := e.store.GetProject(ctx, sqlite.DefaultProjectID)
		if err != nil {
			t.Fatal(err)
		}
		if target >= 0 && target <= 1000 {
			if w.Code != http.StatusOK || after.Defaults.AutoPrewarmTarget != target || after.ConfigVersion != before.ConfigVersion+1 {
				t.Fatalf("target not saved with version guard: %d %s", w.Code, w.Body.String())
			}
		} else if w.Code != http.StatusBadRequest || after.Defaults.AutoPrewarmTarget != before.Defaults.AutoPrewarmTarget || after.ConfigVersion != before.ConfigVersion {
			t.Fatalf("invalid target mutated project: %d %s", w.Code, w.Body.String())
		}
		if after.Defaults.NetworkReuseCooldownMS != before.Defaults.NetworkReuseCooldownMS {
			t.Fatal("pool target update changed blacklist duration")
		}
	}
}
