package processor

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"

	"xgen-crawler/internal/config"
	"xgen-crawler/pkg/types"
)

// Processor defines content post-processing behaviour after fetching.
type Processor interface {
	Process(ctx context.Context, page *types.Page) (*types.ProcessedContent, error)
}

// HTMLProcessor removes ads and noisy elements before persistence/indexing.
type HTMLProcessor struct {
	opts config.PreprocessConfig
}

// NewHTMLProcessor constructs a processor from configuration.
func NewHTMLProcessor(cfg config.PreprocessConfig) *HTMLProcessor {
	return &HTMLProcessor{opts: cfg}
}

var preservedTextTags = map[string]struct{}{
	"table": {},
	"thead": {},
	"tbody": {},
	"tfoot": {},
	"tr":    {},
	"th":    {},
	"td":    {},
	"ul":    {},
	"ol":    {},
	"li":    {},
}

var blockLevelTags = map[string]struct{}{
	"p":          {},
	"div":        {},
	"section":    {},
	"article":    {},
	"header":     {},
	"footer":     {},
	"h1":         {},
	"h2":         {},
	"h3":         {},
	"h4":         {},
	"h5":         {},
	"h6":         {},
	"li":         {},
	"table":      {},
	"tr":         {},
	"figure":     {},
	"figcaption": {},
}

// Process sanitises HTML by removing ad selectors and unwanted nodes and derives textual artefacts.
func (p *HTMLProcessor) Process(ctx context.Context, page *types.Page) (*types.ProcessedContent, error) {
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

	htmlStr, err := doc.Html()
	if err != nil {
		return nil, fmt.Errorf("serialise html: %w", err)
	}

	if p.opts.TrimWhitespace {
		htmlStr = strings.TrimSpace(htmlStr)
	}

	extracted, markdown, err := buildTextAndMarkdown(htmlStr)
	if err != nil {
		return nil, err
	}

	return &types.ProcessedContent{
		CleanHTML:     []byte(htmlStr),
		ExtractedText: extracted,
		Markdown:      markdown,
	}, nil
}

func buildTextAndMarkdown(cleanHTML string) (string, string, error) {
	root, err := html.Parse(strings.NewReader(cleanHTML))
	if err != nil {
		return "", "", fmt.Errorf("parse processed html: %w", err)
	}

	contentRoot := findContentRoot(root)

	textAcc := newTextAccumulator()
	for child := contentRoot.FirstChild; child != nil; child = child.NextSibling {
		accumulateExtractedText(child, textAcc)
	}
	extracted := collapseBlankLines(strings.TrimSpace(textAcc.String()))

	mdAcc := newMarkdownAccumulator()
	state := &mdState{}
	for child := contentRoot.FirstChild; child != nil; child = child.NextSibling {
		renderMarkdownNode(child, state, mdAcc)
	}
	markdown := collapseBlankLines(strings.TrimSpace(mdAcc.String()))

	return extracted, markdown, nil
}

func findContentRoot(node *html.Node) *html.Node {
	if node == nil {
		return nil
	}
	if body := findFirstElement(node, "body"); body != nil {
		return body
	}
	if htmlNode := findFirstElement(node, "html"); htmlNode != nil {
		return htmlNode
	}
	return node
}

func findFirstElement(node *html.Node, tag string) *html.Node {
	if node == nil {
		return nil
	}
	if node.Type == html.ElementNode && strings.EqualFold(node.Data, tag) {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if found := findFirstElement(child, tag); found != nil {
			return found
		}
	}
	return nil
}

type textAccumulator struct {
	builder   strings.Builder
	lastRune  rune
	hasLast   bool
	lastWasNL bool
}

func newTextAccumulator() *textAccumulator {
	return &textAccumulator{}
}

func (t *textAccumulator) String() string {
	return t.builder.String()
}

func (t *textAccumulator) append(value string) {
	if value == "" {
		return
	}
	t.builder.WriteString(value)
	for _, r := range value {
		t.lastRune = r
		t.hasLast = true
		t.lastWasNL = r == '\n'
	}
}

func (t *textAccumulator) ensureSpace() {
	if !t.hasLast || t.lastRune == ' ' || t.lastRune == '\n' {
		return
	}
	t.append(" ")
}

func (t *textAccumulator) ensureNewline() {
	if t.lastWasNL {
		return
	}
	t.append("\n")
}

func (t *textAccumulator) ensureSpaceBeforeText() {
	if !t.hasLast {
		return
	}
	if t.lastRune == ' ' || t.lastRune == '\n' {
		return
	}
	t.append(" ")
}

func accumulateExtractedText(node *html.Node, acc *textAccumulator) {
	if node == nil {
		return
	}
	switch node.Type {
	case html.TextNode:
		text := normalizeWhitespace(node.Data)
		if text == "" {
			return
		}
		acc.ensureSpaceBeforeText()
		acc.append(text)
	case html.ElementNode:
		tag := strings.ToLower(node.Data)
		if tag == "br" {
			acc.ensureNewline()
			return
		}

		if _, ok := blockLevelTags[tag]; ok {
			acc.ensureNewline()
		}

		if _, preserve := preservedTextTags[tag]; preserve {
			acc.append("<" + tag + ">")
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			accumulateExtractedText(child, acc)
		}
		if _, preserve := preservedTextTags[tag]; preserve {
			acc.append("</" + tag + ">")
		}

		switch tag {
		case "td", "th":
			acc.ensureSpace()
		default:
			if _, ok := blockLevelTags[tag]; ok {
				acc.ensureNewline()
			}
		}
	}
}

type listFrame struct {
	ordered bool
	index   int
}

type mdState struct {
	listStack   []listFrame
	inCodeBlock bool
}

type markdownAccumulator struct {
	builder          strings.Builder
	lastRune         rune
	hasLast          bool
	trailingNewlines int
}

func newMarkdownAccumulator() *markdownAccumulator {
	return &markdownAccumulator{}
}

func (m *markdownAccumulator) String() string {
	return m.builder.String()
}

func (m *markdownAccumulator) append(value string) {
	if value == "" {
		return
	}
	m.builder.WriteString(value)
	for _, r := range value {
		m.lastRune = r
		m.hasLast = true
		if r == '\n' {
			m.trailingNewlines++
		} else {
			m.trailingNewlines = 0
		}
	}
}

func (m *markdownAccumulator) ensureSpace() {
	if !m.hasLast || m.trailingNewlines > 0 || m.lastRune == ' ' {
		return
	}
	m.append(" ")
}

func (m *markdownAccumulator) ensureLineBreak() {
	if m.trailingNewlines >= 1 {
		return
	}
	m.append("\n")
}

func (m *markdownAccumulator) ensureBlankLine() {
	if m.trailingNewlines >= 2 {
		return
	}
	if m.trailingNewlines == 0 {
		m.append("\n")
	}
	if m.trailingNewlines == 1 {
		m.append("\n")
	}
}

func renderMarkdownNode(node *html.Node, state *mdState, acc *markdownAccumulator) {
	if node == nil {
		return
	}
	switch node.Type {
	case html.TextNode:
		text := normalizeWhitespace(node.Data)
		if text == "" {
			return
		}
		if state.inCodeBlock {
			acc.append(text)
			return
		}
		acc.ensureSpace()
		acc.append(text)
	case html.ElementNode:
		tag := strings.ToLower(node.Data)
		switch tag {
		case "br":
			acc.append("  \n")
		case "p", "div", "section", "article", "header", "footer":
			acc.ensureBlankLine()
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			acc.ensureBlankLine()
		case "h1", "h2", "h3", "h4", "h5", "h6":
			level := int(tag[1] - '0')
			if level < 1 {
				level = 1
			}
			if level > 6 {
				level = 6
			}
			acc.ensureBlankLine()
			acc.append(strings.Repeat("#", level) + " ")
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			acc.ensureBlankLine()
		case "strong", "b":
			acc.append("**")
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			acc.append("**")
		case "em", "i":
			acc.append("_")
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			acc.append("_")
		case "code":
			text := normalizeWhitespace(getTextContent(node))
			if text == "" {
				return
			}
			acc.append("`" + text + "`")
		case "pre":
			acc.ensureBlankLine()
			acc.append("```\n")
			state.inCodeBlock = true
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			state.inCodeBlock = false
			if acc.trailingNewlines == 0 {
				acc.append("\n")
			}
			acc.append("```\n")
			acc.trailingNewlines = 1
		case "a":
			href := getAttr(node, "href")
			text := normalizeWhitespace(getTextContent(node))
			if text == "" {
				text = href
			}
			if text == "" {
				return
			}
			if href == "" {
				acc.append(text)
			} else {
				acc.append("[" + text + "](" + href + ")")
			}
		case "ul", "ol":
			ordered := tag == "ol"
			state.listStack = append(state.listStack, listFrame{ordered: ordered})
			acc.ensureBlankLine()
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			state.listStack = state.listStack[:len(state.listStack)-1]
			acc.ensureBlankLine()
		case "li":
			if len(state.listStack) == 0 {
				state.listStack = append(state.listStack, listFrame{ordered: false})
			}
			frame := &state.listStack[len(state.listStack)-1]
			frame.index++
			acc.ensureLineBreak()
			indent := strings.Repeat("  ", len(state.listStack)-1)
			marker := "- "
			if frame.ordered {
				marker = fmt.Sprintf("%d. ", frame.index)
			}
			acc.append(indent + marker)
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
			acc.ensureLineBreak()
		case "table":
			acc.ensureBlankLine()
			tableMD := renderTableToMarkdown(node)
			if tableMD != "" {
				acc.append(tableMD)
				if acc.trailingNewlines == 0 {
					acc.append("\n")
				}
			}
			acc.ensureBlankLine()
		default:
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderMarkdownNode(child, state, acc)
			}
		}
	}
}

func renderTableToMarkdown(table *html.Node) string {
	rows := collectTableRows(table)
	if len(rows) == 0 {
		return ""
	}
	headerIdx := -1
	for i, row := range rows {
		if row.header {
			headerIdx = i
			break
		}
	}
	if headerIdx == -1 {
		headerIdx = 0
		rows[0].header = true
	}

	headerCells := rows[headerIdx].cells
	colCount := len(headerCells)
	if colCount == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("| ")
	for i, cell := range headerCells {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString(cell)
	}
	b.WriteString(" |\n| ")
	for i := 0; i < colCount; i++ {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString("---")
	}
	b.WriteString(" |\n")

	for i, row := range rows {
		if i == headerIdx {
			continue
		}
		b.WriteString("| ")
		for j := 0; j < colCount; j++ {
			if j > 0 {
				b.WriteString(" | ")
			}
			if j < len(row.cells) {
				b.WriteString(row.cells[j])
			}
		}
		b.WriteString(" |\n")
	}

	return b.String()
}

type tableRow struct {
	cells  []string
	header bool
}

func collectTableRows(node *html.Node) []tableRow {
	var rows []tableRow
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, header bool) {
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			if child.Type != html.ElementNode {
				continue
			}
			tag := strings.ToLower(child.Data)
			switch tag {
			case "thead":
				walk(child, true)
			case "tbody", "tfoot":
				walk(child, header)
			case "tr":
				row := tableRow{header: header}
				for cell := child.FirstChild; cell != nil; cell = cell.NextSibling {
					if cell.Type != html.ElementNode {
						continue
					}
					cellTag := strings.ToLower(cell.Data)
					if cellTag != "td" && cellTag != "th" {
						continue
					}
					if cellTag == "th" {
						row.header = true
					}
					row.cells = append(row.cells, normalizeWhitespace(getTextContent(cell)))
				}
				if len(row.cells) > 0 {
					rows = append(rows, row)
				}
			default:
				walk(child, header)
			}
		}
	}
	walk(node, false)
	return rows
}

func collapseBlankLines(s string) string {
	lines := strings.Split(s, "\n")
	result := make([]string, 0, len(lines))
	blank := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			blank++
			if blank > 1 {
				continue
			}
			result = append(result, "")
			continue
		}
		blank = 0
		result = append(result, strings.TrimRight(line, " \t"))
	}
	return strings.TrimSpace(strings.Join(result, "\n"))
}

func normalizeWhitespace(s string) string {
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

func getTextContent(node *html.Node) string {
	if node == nil {
		return ""
	}
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		switch n.Type {
		case html.TextNode:
			text := normalizeWhitespace(n.Data)
			if text != "" {
				if b.Len() > 0 {
					b.WriteString(" ")
				}
				b.WriteString(text)
			}
		case html.ElementNode:
			for child := n.FirstChild; child != nil; child = child.NextSibling {
				walk(child)
			}
		}
	}
	walk(node)
	return b.String()
}

func getAttr(node *html.Node, attr string) string {
	for _, a := range node.Attr {
		if strings.EqualFold(a.Key, attr) {
			return a.Val
		}
	}
	return ""
}
