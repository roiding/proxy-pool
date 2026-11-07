package sites

import (
	"bytes"
	"strings"

	"golang.org/x/net/html"
)

// The 89ip API includes a known advert banner/script before a bare br-list
// and a fixed promotional footer. Only recognize that declared shape; ignore
// scripts as data and never regex-extract endpoints from arbitrary HTML.
func ip89AdvertisedListContent(body []byte) ([]byte, bool) {
	if !bytes.Contains(body, []byte(`data-type="img"`)) || !bytes.Contains(body, []byte(`id="adarea"`)) {
		return nil, false
	}
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, false
	}
	var bodyNode *html.Node
	var find func(*html.Node)
	find = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "body" {
			bodyNode = n
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			find(c)
		}
	}
	find(root)
	if bodyNode == nil {
		return nil, false
	}
	var text strings.Builder
	sawBanner, sawArea, sawRow := false, false, false
	for n := bodyNode.FirstChild; n != nil; n = n.NextSibling {
		if n.Type == html.TextNode {
			text.WriteString(n.Data)
			if strings.Contains(n.Data, ":") {
				sawRow = true
			}
			continue
		}
		if n.Type == html.CommentNode {
			continue
		}
		if n.Type != html.ElementNode {
			return nil, false
		}
		if n.Data == "br" {
			text.WriteByte('\n')
			continue
		}
		if sawRow {
			return nil, false
		}
		switch n.Data {
		case "a":
			if attr(n, "href") != "https://www.qiyunip.com/" || attr(n, "data-type") != "img" || n.FirstChild == nil || n.FirstChild.Data != "img" || attr(n.FirstChild, "src") != "img/hfad.png" {
				return nil, false
			}
			sawBanner = true
		case "div":
			if attr(n, "id") != "adarea" {
				return nil, false
			}
			sawArea = true
		case "script":
			src := attr(n, "src")
			if src != "" && src != "js/jquery.min.js" {
				return nil, false
			}
			// No script execution. Inline known advert controls may be present only before rows.
			if src == "" && (!strings.Contains(textContent(n), "#adarea") || !strings.Contains(textContent(n), "#adclose")) {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	if !sawBanner || !sawArea || !sawRow {
		return nil, false
	}
	rows := strings.TrimSpace(text.String())
	const footer = "更好用的代理ip请访问：www.qiyunip.com"
	rows = strings.TrimSpace(strings.TrimSuffix(rows, footer))
	// Every remaining nonblank row must be an entire endpoint token.
	hasRow := false
	for _, line := range strings.Split(rows, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !pureBodyRowPattern.MatchString(line) {
			return nil, false
		}
		hasRow = true
	}
	return []byte(rows), hasRow
}
