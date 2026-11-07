package validation

import "strings"

// FailureReason returns fixed diagnostic labels only. Raw proxy URLs,
// credentials, IPs, certificates and upstream response text stay private.
func FailureReason(f *Failure) string {
	if f == nil {
		return ""
	}
	detail := strings.ToLower(f.Detail)
	switch {
	case f.Kind == FailAuth:
		return "proxy_authentication"
	case strings.Contains(detail, "connection refused"):
		return "connection_refused"
	case strings.Contains(detail, "timeout") || strings.Contains(detail, "deadline exceeded"):
		return "timeout"
	case strings.Contains(detail, "network is unreachable") || strings.Contains(detail, "no route to host"):
		return "network_unreachable"
	case strings.Contains(detail, "certificate"):
		return "tls_certificate_rejected"
	case strings.Contains(detail, "tls handshake"):
		return "target_tls_handshake"
	case strings.Contains(detail, "resolve target") || strings.Contains(detail, "resolve proxy"):
		return "dns_failure"
	case strings.Contains(detail, "read connect response") || strings.Contains(detail, "malformed"):
		return "invalid_proxy_protocol"
	case strings.Contains(detail, "reset by peer") || strings.Contains(detail, "eof"):
		return "connection_closed"
	case strings.Contains(detail, "judge returned status"):
		return "judge_http_status"
	case strings.Contains(detail, "nonce mismatch") || strings.Contains(detail, "signature mismatch"):
		return "judge_verification_failed"
	default:
		switch f.Kind {
		case FailProxy:
			return "proxy_error"
		case FailJudge:
			return "judge_error"
		case FailTarget:
			return "target_error"
		default:
			return "probe_budget"
		}
	}
}
