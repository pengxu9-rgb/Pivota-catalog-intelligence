/**
 * Brand-authored copy that Shopify themes render OUTSIDE products.json body_html.
 *
 * Measured 2026-09-29 over the served brand-store products whose description is under 200 chars:
 * on 12 storefronts the product page carries the brand's own descriptive sections that no
 * collector reads -- Supergoop's "Why we made it" / "How it feels", Merit's "WHAT IT IS" /
 * "WHAT IT DOES" cards, Make Up For Ever's "Why you will love it", Glasshouse's "Inspiration",
 * the "Description" / "Details" / "Benefits" accordions on Jurlique, Soko Glam, Anua and
 * Celimax, Celimax's and Tatcha's prose blocks. Themes differ on every one of those, so this
 * does not match theme selectors: it anchors on a HEADING whose text is a descriptive section
 * name, and takes the content that belongs to that heading.
 *
 * Pure (HTML in, sections out), so it runs on the page HTML the Shopify-direct path already
 * fetches, without a browser.
 */
import type { ExtractedProductDetailSection } from "./types";

export const THEME_SECTION_SOURCE_KIND = "page_theme_section";

/** Heading texts that name a descriptive section, matched on the heading's lowercased words. */
const BRAND_COPY_HEADING_RE = new RegExp(
  "^(?:" +
    [
      "(?:product )?(?:details|overview|description)",
      "(?:key |product )?benefits?",
      "what it (?:is|does)",
      "why (?:we made it|we love it|you ll love it|you will love it)",
      "how it feels",
      "highlights",
      "inspiration",
    ].join("|") +
    ")$",
);

const RAW_TEXT_TAGS = new Set(["script", "style", "textarea", "noscript", "title", "xmp"]);
const SKIPPED_CONTENT_TAGS = new Set(["template", "svg", "iframe", "select", "button"]);
const VOID_TAGS = new Set([
  "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr",
]);
const BLOCK_TAGS = new Set([
  "p", "div", "li", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "section", "article", "details", "summary",
  "dd", "dt", "dl", "table", "tr", "td", "th", "blockquote", "br", "hr", "header", "footer", "main", "aside",
]);
const HEADING_TAG_RE = /^h[2-6]$/;
const HEADING_CLASS_RE = /(?:^|[\s_-])(?:h[1-6]|title|heading|subheading|label|summary__title)(?:$|[\s_-])/i;
/** Page regions that are not the product's copy: chrome, commerce widgets, other products, reviews. */
const EXCLUDED_REGION_TAGS = new Set(["header", "nav", "footer", "form"]);
const EXCLUDED_REGION_RE =
  /(?:^|[\s_-])(?:header|nav|navigation|menu|mega-menu|footer|drawer|cart|cookie|consent|newsletter|popup|announcement|breadcrumbs?|search|reviews?|okendo|yotpo|judgeme|stamped|loox|recommend(?:ations?|ed)?|related|upsell|cross-sell|recently|you-may|complete-the-look|shop-the)(?:$|[\s_-])/i;

const MIN_BODY_CHARS = 60;
const MIN_BODY_WORDS = 8;
const MAX_BODY_CHARS = 2000;
const MAX_SECTIONS = 12;
const MAX_CLIMBS = 2;

type Node = {
  tag: string;
  attrs: string;
  children: Array<Node | string>;
  parent: Node | null;
};

function decodeEntities(value: string): string {
  return value
    .replace(/&#x([0-9a-f]+);/gi, (m, hex) => safeCodePoint(Number.parseInt(hex, 16), m))
    .replace(/&#([0-9]+);/g, (m, dec) => safeCodePoint(Number.parseInt(dec, 10), m))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&rsquo;/gi, "’")
    .replace(/&lsquo;/gi, "‘")
    .replace(/&ldquo;/gi, "“")
    .replace(/&rdquo;/gi, "”")
    .replace(/&ndash;/gi, "–")
    .replace(/&mdash;/gi, "—");
}

function safeCodePoint(codePoint: number, fallback: string): string {
  try {
    return Number.isFinite(codePoint) ? String.fromCodePoint(codePoint) : fallback;
  } catch {
    return fallback;
  }
}

function attrValue(attrs: string, name: string): string {
  const match = attrs.match(new RegExp(`(?:^|\\s)${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, "i"));
  return match ? match[1] ?? match[2] ?? match[3] ?? "" : "";
}

/** A forgiving tree: unclosed tags close with their parent, stray end tags are ignored, and the
 *  content of script/style/template/svg/button/select is dropped. */
export function parseHtmlTree(html: string): Node {
  const root: Node = { tag: "#root", attrs: "", children: [], parent: null };
  let current = root;
  const tokenRe = /<!--[\s\S]*?-->|<!\[CDATA\[[\s\S]*?\]\]>|<!doctype[^>]*>|<\/?([a-zA-Z][\w:-]*)((?:[^>"']|"[^"]*"|'[^']*')*)>|[^<]+|</g;
  let skipDepth = 0;
  let skipTag = "";
  let match: RegExpExecArray | null;
  while ((match = tokenRe.exec(html))) {
    const token = match[0];
    const tag = (match[1] || "").toLowerCase();
    if (skipDepth > 0) {
      if (tag === skipTag) {
        if (token.startsWith("</")) skipDepth -= 1;
        else if (!token.endsWith("/>")) skipDepth += 1;
      }
      continue;
    }
    if (!tag) {
      if (!token.startsWith("<!")) {
        const text = token === "<" ? "<" : token;
        current.children.push(decodeEntities(text));
      }
      continue;
    }
    if (token.startsWith("</")) {
      let node: Node | null = current;
      while (node && node.tag !== tag) node = node.parent;
      if (node && node.parent) current = node.parent;
      continue;
    }
    if (RAW_TEXT_TAGS.has(tag)) {
      // Raw text: nothing inside is markup (an inline script may well contain "<script"), so jump
      // straight past the closing tag instead of counting nested ones.
      if (!token.endsWith("/>")) {
        const close = html.toLowerCase().indexOf(`</${tag}`, tokenRe.lastIndex);
        const end = close < 0 ? -1 : html.indexOf(">", close);
        tokenRe.lastIndex = end < 0 ? html.length : end + 1;
      }
      continue;
    }
    if (SKIPPED_CONTENT_TAGS.has(tag)) {
      if (!token.endsWith("/>")) {
        skipDepth = 1;
        skipTag = tag;
      }
      continue;
    }
    const node: Node = { tag, attrs: match[2] || "", children: [], parent: current };
    current.children.push(node);
    if (!VOID_TAGS.has(tag) && !token.endsWith("/>")) current = node;
  }
  return root;
}

function textOf(node: Node | string, out: string[] = []): string[] {
  if (typeof node === "string") {
    out.push(node);
    return out;
  }
  const block = BLOCK_TAGS.has(node.tag);
  if (block) out.push("\n");
  for (const child of node.children) textOf(child, out);
  if (block) out.push("\n");
  return out;
}

function normalizeText(parts: string[]): string {
  return parts
    .join("")
    .split("\n")
    .map((line) => line.replace(/[\s   ]+/g, " ").trim())
    .filter((line) => line.length > 0)
    .join("\n");
}

function nodeText(node: Node | string): string {
  return normalizeText(textOf(node));
}

function headingWords(text: string): string {
  return (text.toLowerCase().match(/[a-z0-9]+/g) || []).join(" ");
}

function isExcludedRegion(node: Node): boolean {
  for (let n: Node | null = node; n; n = n.parent) {
    if (EXCLUDED_REGION_TAGS.has(n.tag)) return true;
    const marker = `${attrValue(n.attrs, "class")} ${attrValue(n.attrs, "id")}`;
    if (marker.trim() && EXCLUDED_REGION_RE.test(marker)) return true;
  }
  return false;
}

function isHeadingLike(node: Node): boolean {
  if (HEADING_TAG_RE.test(node.tag) || node.tag === "summary") return true;
  return HEADING_CLASS_RE.test(attrValue(node.attrs, "class"));
}

function containsHeading(node: Node | string): boolean {
  if (typeof node === "string") return false;
  if (HEADING_TAG_RE.test(node.tag) || node.tag === "summary") return true;
  return node.children.some((child) => containsHeading(child));
}

function ancestor(node: Node, tag: string): Node | null {
  for (let n: Node | null = node; n; n = n.parent) if (n.tag === tag) return n;
  return null;
}

/** The content that belongs to `heading`: an accordion's panel, else the siblings after it up to
 *  the next heading -- climbing out of a wrapper that holds nothing but the heading. */
function bodyFor(heading: Node): string {
  const summary = heading.tag === "summary" ? heading : ancestor(heading, "summary");
  const details = summary ? ancestor(summary, "details") : null;
  if (summary && details) {
    return nodeText({ ...details, children: details.children.filter((child) => child !== summary) });
  }
  let anchor: Node = heading;
  for (let climb = 0; climb <= MAX_CLIMBS && anchor.parent; climb += 1) {
    const siblings = anchor.parent.children;
    const parts: string[] = [];
    for (const sibling of siblings.slice(siblings.indexOf(anchor) + 1)) {
      if (typeof sibling !== "string" && (isHeadingLike(sibling) || containsHeading(sibling))) break;
      textOf(sibling, parts);
    }
    const text = normalizeText(parts);
    if (text) return text;
    anchor = anchor.parent;
  }
  return "";
}

function clip(text: string): string {
  if (text.length <= MAX_BODY_CHARS) return text;
  const cut = text.slice(0, MAX_BODY_CHARS);
  const end = Math.max(cut.lastIndexOf(". "), cut.lastIndexOf("\n"));
  return (end > MAX_BODY_CHARS / 3 ? cut.slice(0, end + 1) : cut).trim();
}

/**
 * The brand's descriptive sections on a product page: one per heading whose text names one
 * (BRAND_COPY_HEADING_RE), outside page chrome, commerce widgets, reviews and other products,
 * with at least MIN_BODY_CHARS / MIN_BODY_WORDS of its own text. Identical sections (a theme
 * renders the same block for mobile and desktop) are kept once.
 */
export function extractShopifyThemeBrandSections(html: string | undefined): ExtractedProductDetailSection[] {
  if (!html || !html.trim()) return [];
  const root = parseHtmlTree(html);
  const sections: ExtractedProductDetailSection[] = [];
  const seen = new Set<string>();
  const visit = (node: Node) => {
    if (sections.length >= MAX_SECTIONS) return;
    if (isHeadingLike(node)) {
      const heading = nodeText(node).split("\n")[0].replace(/[:?+]+$/, "").trim();
      if (heading && BRAND_COPY_HEADING_RE.test(headingWords(heading))) {
        if (!isExcludedRegion(node)) {
          const body = clip(bodyFor(node));
          const words = body.match(/[A-Za-z][A-Za-z'’-]*/g) || [];
          const key = `${headingWords(heading)}|${body.toLowerCase()}`;
          if (body.length >= MIN_BODY_CHARS && words.length >= MIN_BODY_WORDS && !seen.has(key)) {
            seen.add(key);
            sections.push({ heading, body, source_kind: THEME_SECTION_SOURCE_KIND });
          }
        }
        if (node.tag === "summary" || HEADING_TAG_RE.test(node.tag)) return;
      }
    }
    for (const child of node.children) if (typeof child !== "string") visit(child);
  };
  visit(root);
  return sections;
}

/** Whether `sections` already hold a descriptive one (the same heading rule) with real text. */
export function hasBrandDescriptiveSection(sections: ExtractedProductDetailSection[] | undefined): boolean {
  return (Array.isArray(sections) ? sections : []).some((section) => {
    const heading = String(section?.heading || "").split("\n")[0].replace(/[:?+]+$/, "").trim();
    return BRAND_COPY_HEADING_RE.test(headingWords(heading)) && String(section?.body || "").trim().length >= MIN_BODY_CHARS;
  });
}
