package email

import (
	"errors"
	"regexp"
	"strings"

	"engageiq-workers/pkg/types"
)

// Renderer prepares the email body for sending. For now this is a thin layer
// that ensures both HTML and text bodies are populated (generating a plain
// text fallback from HTML when missing).
type Renderer struct{}

func NewRenderer() *Renderer { return &Renderer{} }

// Rendered is the output of the renderer.
type Rendered struct {
	Subject string
	HTML    string
	Text    string
}

var (
	tagRE   = regexp.MustCompile(`<[^>]*>`)
	wsRE    = regexp.MustCompile(`[ \t\r\f\v]+`)
	emptyRE = regexp.MustCompile(`\n{3,}`)
)

// Render prepares HTML and text bodies for sending. Returns an error if the
// payload has no body content at all.
func (r *Renderer) Render(p *types.TransactionalEmailPayload) (Rendered, error) {
	out := Rendered{
		Subject: strings.TrimSpace(p.Subject),
		HTML:    p.HTML,
		Text:    p.Text,
	}
	if out.HTML == "" && out.Text == "" {
		return out, errors.New("renderer: payload has no html or text body")
	}
	if out.Text == "" {
		out.Text = htmlToText(out.HTML)
	}
	return out, nil
}

// htmlToText is a deliberately simple plain-text fallback. It is not a
// substitute for a proper HTML-to-text library; senders should prefer to
// supply explicit text.
func htmlToText(html string) string {
	s := tagRE.ReplaceAllString(html, " ")
	s = strings.NewReplacer(
		"&nbsp;", " ",
		"&amp;", "&",
		"&lt;", "<",
		"&gt;", ">",
		"&quot;", `"`,
		"&#39;", "'",
	).Replace(s)
	s = wsRE.ReplaceAllString(s, " ")
	s = emptyRE.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}
