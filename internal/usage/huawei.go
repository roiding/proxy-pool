package usage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const huaweiReaderNote = "通过官方 RMS 查询 L 实例流量包 ID，再调用 BSS ListFreeResourceUsages 读取剩余额度，按云端返回的实际使用周期告警。查询失败时保留上次数据。"

var (
	huaweiRegionPattern   = regexp.MustCompile(`^[a-z0-9]+-[a-z0-9]+-[0-9]+$`)
	huaweiResourcePattern = regexp.MustCompile(`^[a-fA-F0-9]{24,32}$`)
	huaweiPackagePattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	huaweiCodePattern     = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)
)

// The official RMS resource graph supplies the CBC free-resource physical ID.
// The BSS package-list API does not include the Flexus L instance bundle.
type huaweiReader struct {
	load   func(context.Context) (settings, error)
	now    func() time.Time
	client *http.Client
}

func newHuaweiReader(s *Service) *huaweiReader {
	return &huaweiReader{
		load: s.loadSettings, now: s.Now,
		client: &http.Client{
			Transport: &http.Transport{
				Proxy: nil, MaxIdleConns: 3, IdleConnTimeout: 30 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second,
				MaxResponseHeaderBytes: 32 << 10,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func huaweiConfigurationError(st settings) error {
	if st.HuaweiAK == "" || st.HuaweiSK == "" || st.HuaweiRegion == "" || (st.HuaweiResource == "" && st.HuaweiPackage == "") {
		return ErrUsageReaderUnavailable
	}
	if !huaweiRegionPattern.MatchString(st.HuaweiRegion) {
		return errors.New("华为云区域代码格式不正确")
	}
	if st.HuaweiResource != "" && !huaweiResourcePattern.MatchString(st.HuaweiResource) {
		return errors.New("华为云资源 ID 应填写 Flexus L 实例 ID")
	}
	if st.HuaweiPackage != "" && !huaweiPackagePattern.MatchString(st.HuaweiPackage) {
		return errors.New("华为云流量包 ID 应为 1 至 64 位字母、数字、下划线或连字符")
	}
	return nil
}

func (r *huaweiReader) ReadMonthlyUsage(ctx context.Context) (Snapshot, error) {
	st, err := r.load(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if err := huaweiConfigurationError(st); err != nil {
		return Snapshot{}, err
	}
	packageID := st.HuaweiPackage
	if packageID == "" {
		packageID, err = r.discoverPackage(ctx, st)
		if err != nil {
			return Snapshot{}, err
		}
	}
	var result huaweiPackages
	if err := r.request(ctx, st,
		"https://bss.myhuaweicloud.com/v2/payments/free-resources/usages/details/query",
		struct {
			IDs []string `json:"free_resource_ids"`
		}{[]string{packageID}}, &result); err != nil {
		return Snapshot{}, err
	}
	return result.snapshot(packageID, r.now().UTC())
}

func (r *huaweiReader) discoverPackage(ctx context.Context, st settings) (string, error) {
	var projects struct {
		Projects []struct {
			Name     string `json:"name"`
			ParentID string `json:"parent_id"`
		} `json:"projects"`
	}
	if err := r.request(ctx, st, "https://iam.myhuaweicloud.com/v3/projects?name="+url.QueryEscape(st.HuaweiRegion), nil, &projects); err != nil {
		return "", err
	}
	domainID := ""
	for _, p := range projects.Projects {
		if p.Name == st.HuaweiRegion && p.ParentID != "" {
			if domainID != "" && domainID != p.ParentID {
				return "", errors.New("华为云区域返回了多个账号，无法确定套餐归属")
			}
			domainID = p.ParentID
		}
	}
	if domainID == "" || !huaweiResourcePattern.MatchString(domainID) {
		return "", errors.New("华为云账号中未找到所配置的区域项目；请检查区域和 IAM 项目读取权限")
	}
	marker := ""
	seen := map[string]bool{}
	for page := 0; page < 20; page++ {
		query := url.Values{"type": {"hcss.l-instance"}, "region_id": {st.HuaweiRegion}, "id": {st.HuaweiResource}, "limit": {"100"}}
		if marker != "" {
			query.Set("marker", marker)
		}
		var result struct {
			Resources []struct {
				ID         string `json:"id"`
				Region     string `json:"region_id"`
				State      string `json:"state"`
				Properties struct {
					Resources []struct {
						Type string `json:"logical_resource_type"`
						ID   string `json:"physical_resource_id"`
					} `json:"resources"`
				} `json:"properties"`
			} `json:"resources"`
			PageInfo struct {
				NextMarker string `json:"next_marker"`
			} `json:"page_info"`
		}
		endpoint := "https://rms.myhuaweicloud.com/v1/resource-manager/domains/" + domainID + "/all-resources?" + query.Encode()
		if err := r.request(ctx, st, endpoint, nil, &result); err != nil {
			return "", err
		}
		for _, instance := range result.Resources {
			if instance.ID != st.HuaweiResource || instance.Region != st.HuaweiRegion || strings.EqualFold(instance.State, "Deleted") {
				continue
			}
			id := ""
			for _, resource := range instance.Properties.Resources {
				if resource.Type != "huaweicloudinternal_cbc_freeresource" {
					continue
				}
				if !huaweiPackagePattern.MatchString(resource.ID) {
					return "", errors.New("华为云实例返回了无效的流量包 ID")
				}
				if id != "" && id != resource.ID {
					return "", errors.New("L 实例关联多个流量包，请填写流量包 ID 后重试")
				}
				id = resource.ID
			}
			if id == "" {
				return "", errors.New("L 实例没有固定流量包；该读取器适用于通用型 Flexus L 实例")
			}
			return id, nil
		}
		marker = result.PageInfo.NextMarker
		if marker == "" {
			return "", errors.New("华为云资源列表未找到对应区域的 L 实例；请检查实例 ID、区域和 RMS 读取权限")
		}
		if seen[marker] {
			return "", errors.New("华为云资源列表分页标记重复，已停止查询")
		}
		seen[marker] = true
	}
	return "", errors.New("华为云资源列表超过分页查询上限，请直接填写流量包 ID")
}

type huaweiPackages struct {
	Details []huaweiPackage `json:"free_resources"`
}

type huaweiPackage struct {
	ID       string          `json:"free_resource_id"`
	Start    string          `json:"start_time"`
	End      string          `json:"end_time"`
	Amount   json.RawMessage `json:"amount"`
	Original json.RawMessage `json:"original_amount"`
	Measure  *int            `json:"measure_id"`
}

func (p huaweiPackages) snapshot(packageID string, now time.Time) (Snapshot, error) {
	var selected *huaweiPackage
	var start, end time.Time
	for i := range p.Details {
		pkg := &p.Details[i]
		if pkg.ID != packageID {
			return Snapshot{}, errors.New("华为云返回了未请求的流量包，已拒绝保存")
		}
		a, err := parseHuaweiTime(pkg.Start)
		if err != nil {
			return Snapshot{}, err
		}
		b, err := parseHuaweiTime(pkg.End)
		if err != nil || !b.After(a) {
			return Snapshot{}, errors.New("华为云返回了无效的套餐使用周期")
		}
		if now.Before(a) || !now.Before(b) {
			continue
		}
		if selected != nil {
			return Snapshot{}, errors.New("华为云返回多个当前周期额度，无法确定用量")
		}
		selected, start, end = pkg, a, b
	}
	if selected == nil {
		return Snapshot{}, errors.New("华为云未返回当前周期的流量包；请检查流量包 ID 和有效期")
	}
	// BSS measure IDs 10, 17 and 93 all identify GB (verified with ListMeasureUnits).
	if selected.Measure == nil || (*selected.Measure != 10 && *selected.Measure != 17 && *selected.Measure != 93) {
		return Snapshot{}, errors.New("华为云套餐缺少 GB 度量单位，已拒绝保存")
	}
	remaining, err := huaweiAmount(selected.Amount)
	if err != nil {
		return Snapshot{}, err
	}
	total, err := huaweiAmount(selected.Original)
	if err != nil || total <= 0 || remaining > total {
		return Snapshot{}, errors.New("华为云套餐额度不完整或不一致，已拒绝保存")
	}
	return Snapshot{PeriodStart: start, PeriodEnd: end, AsOf: now, UsedGB: total - remaining, TotalGB: total, RemainingGB: &remaining,
		Source: "Huawei BSS ListFreeResourceUsages（官方套餐余量）",
		Note: fmt.Sprintf("流量包 ID %s；按套餐实际使用周期统计；接口未返回数据更新时间，采样时间为查询时间，云端用量可能有延迟。",
			packageID)}, nil
}

func parseHuaweiTime(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errors.New("华为云套餐缺少有效的使用周期时间")
}

func huaweiAmount(raw json.RawMessage) (float64, error) {
	value := strings.TrimSpace(string(raw))
	if strings.HasPrefix(value, `"`) {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, errors.New("华为云套餐返回了无效的额度")
		}
		value = text
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return 0, errors.New("华为云套餐额度缺失或无效，已拒绝保存")
	}
	return n, nil
}

func (r *huaweiReader) request(ctx context.Context, st settings, endpoint string, body, out any) error {
	var payload []byte
	var err error
	method := http.MethodGet
	if body != nil {
		method = http.MethodPost
		payload, err = json.Marshal(body)
		if err != nil {
			return errors.New("华为云查询请求无法编码")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return errors.New("华为云查询请求无效")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("region", st.HuaweiRegion)
	signHuaweiRequest(req, payload, st.HuaweiAK, st.HuaweiSK, r.now())
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("华为云查询连接失败（%s）", urlErrorKind(err))
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return errors.New("华为云查询响应不完整或过大")
	}
	var failure struct {
		Code   string `json:"error_code"`
		Action string `json:"auth_action"`
	}
	_ = json.Unmarshal(data, &failure)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || failure.Code != "" {
		if resp.StatusCode == http.StatusForbidden && failure.Code == "CBC.0155" && failure.Action == "bss:bill:view" {
			return errors.New("华为云套餐读取缺少账单只读权限 bss:bill:view（HTTP 403，CBC.0155）")
		}
		code := ""
		if huaweiCodePattern.MatchString(failure.Code) && !strings.Contains(failure.Code, st.HuaweiAK) && !strings.Contains(failure.Code, st.HuaweiSK) {
			code = "，" + failure.Code
		}
		return fmt.Errorf("华为云查询失败（HTTP %d%s）", resp.StatusCode, code)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return errors.New("华为云查询返回了无法识别的响应")
	}
	return nil
}

// Huawei's SDK-HMAC-SHA256 canonical URI always ends in '/'. Sign only
// host and X-Sdk-Date; do not log the authorization header or response text.
func signHuaweiRequest(req *http.Request, body []byte, ak, sk string, now time.Time) {
	date := now.UTC().Format("20060102T150405Z")
	req.Header.Set("X-Sdk-Date", date)
	path := req.URL.EscapedPath()
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	query := req.URL.Query()
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	escape := func(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
	for _, k := range keys {
		values := query[k]
		sort.Strings(values)
		for _, v := range values {
			parts = append(parts, escape(k)+"="+escape(v))
		}
	}
	hash := sha256.Sum256(body)
	canonical := strings.Join([]string{req.Method, path, strings.Join(parts, "&"),
		"host:" + req.URL.Host + "\nx-sdk-date:" + date + "\n", "host;x-sdk-date", hex.EncodeToString(hash[:])}, "\n")
	requestHash := sha256.Sum256([]byte(canonical))
	text := "SDK-HMAC-SHA256\n" + date + "\n" + hex.EncodeToString(requestHash[:])
	mac := hmac.New(sha256.New, []byte(sk))
	_, _ = mac.Write([]byte(text))
	req.Header.Set("Authorization", "SDK-HMAC-SHA256 Access="+ak+", SignedHeaders=host;x-sdk-date, Signature="+hex.EncodeToString(mac.Sum(nil)))
}
