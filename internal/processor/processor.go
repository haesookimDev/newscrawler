package processor

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"newscrawler/internal/config"
	"newscrawler/pkg/types"
)

// Processor defines content post-processing behaviour after fetching.
type Processor interface {
	Process(ctx context.Context, page *types.Page) ([]byte, error)
}

// HTMLProcessor removes ads and noisy elements before persistence/indexing.
type HTMLProcessor struct {
	opts config.PreprocessConfig
}

// NewHTMLProcessor constructs a processor from configuration.
func NewHTMLProcessor(cfg config.PreprocessConfig) *HTMLProcessor {
	return &HTMLProcessor{opts: cfg}
}

// Process sanitises HTML by removing ad selectors and unwanted nodes.
func (p *HTMLProcessor) Process(ctx context.Context, page *types.Page) ([]byte, error) {
	if page == nil {
		return nil, fmt.Errorf("page is nil")
	}
	if len(page.Body) == 0 {
		return nil, fmt.Errorf("page body empty")
	}

	reader := bytes.NewReader(page.Body)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		return nil, fmt.Errorf("parse html: %w", err)
	}

	if p.opts.RemoveScripts {
		doc.Find("script,noscript,iframe").Remove()
	}
	if p.opts.RemoveStyles {
		doc.Find("style,link[rel='stylesheet']").Remove()
	}

	if p.opts.RemoveAds {
		selectors := p.opts.AdSelectors
		if len(selectors) == 0 {
			selectors = []string{"[class*='ad']", "[id*='ad']", "[class*='sponsor']"}
		}
		for _, sel := range selectors {
			doc.Find(sel).Each(func(_ int, s *goquery.Selection) {
				s.Remove()
			})
		}
		for _, cls := range p.opts.ExtraDropClasses {
			sel := fmt.Sprintf(".%s", strings.TrimPrefix(cls, "."))
			doc.Find(sel).Each(func(_ int, s *goquery.Selection) {
				s.Remove()
			})
		}
	}

	html, err := doc.Html()
	if err != nil {
		return nil, fmt.Errorf("serialise html: %w", err)
	}

	if p.opts.TrimWhitespace {
		html = strings.TrimSpace(html)
	}

	return []byte(html), nil
}
