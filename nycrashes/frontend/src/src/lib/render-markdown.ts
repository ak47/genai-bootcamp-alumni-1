const PLACEHOLDER_START = '\uE000';
const PLACEHOLDER_END = '\uE001';

const placeholderPattern = new RegExp(`${PLACEHOLDER_START}(\\d+)${PLACEHOLDER_END}`, 'g');

const fencesRegex = /^```/;
const headingRegex = /^ {0,3}(#{1,6})\s+(.*)$/;
const hrRegex = /^ {0,3}(?:---|\*\*\*|___)\s*$/;
const blockquoteRegex = /^ {0,3}>/;
const unorderedRegex = /^ {0,3}[-*+]\s+/;
const orderedRegex = /^ {0,3}\d+[.)]\s+/;

const isBlank = (line: string) => !line.trim();

const escapeHtml = (value: string): string =>
	value
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;')
		.replace(/'/g, '&#39;');

const escapeAttribute = (value: string): string => escapeHtml(value);

const sanitizeUrl = (value: string): string | null => {
	const trimmed = value.trim();
	if (!trimmed) {
		return null;
	}

	const lower = trimmed.toLowerCase();
	if (lower.startsWith('javascript:') || lower.startsWith('data:')) {
		return null;
	}

	if (trimmed.startsWith('//')) {
		return null;
	}

	return trimmed;
};

export const renderMarkdown = (input: string): string => {
	if (!input || !input.trim()) {
		return '';
	}

	const placeholders: string[] = [];
	const store = (html: string) => {
		const index = placeholders.length;
		placeholders.push(html);
		return `${PLACEHOLDER_START}${index}${PLACEHOLDER_END}`;
	};

	const renderInline = (segment: string): string => {
		if (!segment) {
			return '';
		}

		let text = segment;

		// Inline code first so that other transforms do not affect code payload.
		text = text.replace(/`([^`]+)`/g, (_, code: string) => store(`<code>${escapeHtml(code)}</code>`));

		// Images (rare in chat, but supported).
		text = text.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, altRaw: string, urlRaw: string) => {
			const safeUrl = sanitizeUrl(urlRaw);
			if (!safeUrl) {
				return escapeHtml(altRaw);
			}
			const alt = escapeHtml(altRaw);
			return store(
				`<img src="${escapeAttribute(safeUrl)}" alt="${alt}" loading="lazy" decoding="async" />`
			);
		});

		// Explicit links before auto-linking.
		text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, labelRaw: string, urlRaw: string) => {
			const safeUrl = sanitizeUrl(urlRaw);
			if (!safeUrl) {
				return renderInline(labelRaw);
			}
			const label = renderInline(labelRaw);
			return store(
				`<a href="${escapeAttribute(safeUrl)}" target="_blank" rel="noreferrer noopener">${label}</a>`
			);
		});

		// Auto-link plain URLs (http, https, mailto).
		text = text.replace(
			/(^|[\s(])((?:https?:\/\/|mailto:)[^\s<>]+)(?=$|[\s).,!?])/g,
			(match: string, prefix: string, urlRaw: string) => {
				const safeUrl = sanitizeUrl(urlRaw);
				if (!safeUrl) {
					return match;
				}

				const anchor = `<a href="${escapeAttribute(safeUrl)}" target="_blank" rel="noreferrer noopener">${escapeHtml(urlRaw)}</a>`;
				return `${prefix}${store(anchor)}`;
			}
		);

		text = escapeHtml(text);

		// Strikethrough.
		text = text.replace(
			/(^|[\s>_~(])~~([^\n]+?)~~(?=($|[\s<.,!?:;)\]]))/g,
			(_, prefix: string, content: string, suffix: string) => `${prefix}<del>${content}</del>${suffix ?? ''}`
		);

		// Bold (must run before italic).
		text = text.replace(
			/(^|[\s>_~(])\*\*([^\n]+?)\*\*(?=($|[\s<.,!?:;)\]]))/g,
			(_, prefix: string, content: string, suffix: string) => `${prefix}<strong>${content}</strong>${suffix ?? ''}`
		);

		// Italic (asterisk variant only to avoid conflicts with underscores in attributes).
		text = text.replace(
			/(^|[\s>_~(])\*([^\n]+?)\*(?=($|[\s<.,!?:;)\]]))/g,
			(_, prefix: string, content: string, suffix: string) => `${prefix}<em>${content}</em>${suffix ?? ''}`
		);

		// Remove escaped special characters (e.g., \*).
		text = text.replace(/\\([\\`*_[\]{}()#+\-.!])/g, '$1');

		return text;
	};

	const renderBlocks = (source: string): string => {
		const lines = source.split('\n');
		const output: string[] = [];
		let index = 0;

		while (index < lines.length) {
			const line = lines[index];

			if (isBlank(line)) {
				index += 1;
				continue;
			}

			if (fencesRegex.test(line)) {
				const language = line.slice(3).trim();
				index += 1;
				const codeLines: string[] = [];

				while (index < lines.length && !fencesRegex.test(lines[index])) {
					codeLines.push(lines[index]);
					index += 1;
				}

				if (index < lines.length && fencesRegex.test(lines[index])) {
					index += 1;
				}

				const codeHtml = `<pre><code${language ? ` class="language-${escapeAttribute(language)}"` : ''}>${escapeHtml(codeLines.join('\n'))}</code></pre>`;
				output.push(store(codeHtml));
				continue;
			}

			const headingMatch = line.match(headingRegex);
			if (headingMatch) {
				const [, hashes, text] = headingMatch;
				const level = hashes.length;
				output.push(`<h${level}>${renderInline(text.trim())}</h${level}>`);
				index += 1;
				continue;
			}

			if (hrRegex.test(line)) {
				output.push('<hr />');
				index += 1;
				continue;
			}

			if (blockquoteRegex.test(line)) {
				const quoteLines: string[] = [];
				while (index < lines.length && blockquoteRegex.test(lines[index])) {
					quoteLines.push(lines[index].replace(/^ {0,3}>\s?/, ''));
					index += 1;
				}

				const inner = renderBlocks(quoteLines.join('\n'));
				output.push(`<blockquote>${inner}</blockquote>`);
				continue;
			}

			if (unorderedRegex.test(line)) {
				const items: string[] = [];
				while (index < lines.length && unorderedRegex.test(lines[index])) {
					items.push(lines[index].replace(unorderedRegex, ''));
					index += 1;
				}

				const htmlItems = items.map((item) => `<li>${renderInline(item.trim())}</li>`).join('');
				output.push(`<ul>${htmlItems}</ul>`);
				continue;
			}

			if (orderedRegex.test(line)) {
				const items: string[] = [];
				while (index < lines.length && orderedRegex.test(lines[index])) {
					items.push(lines[index].replace(orderedRegex, ''));
					index += 1;
				}

				const htmlItems = items.map((item) => `<li>${renderInline(item.trim())}</li>`).join('');
				output.push(`<ol>${htmlItems}</ol>`);
				continue;
			}

			const paragraphLines: string[] = [];
			while (
				index < lines.length &&
				!isBlank(lines[index]) &&
				!fencesRegex.test(lines[index]) &&
				!headingRegex.test(lines[index]) &&
				!hrRegex.test(lines[index]) &&
				!blockquoteRegex.test(lines[index]) &&
				!unorderedRegex.test(lines[index]) &&
				!orderedRegex.test(lines[index])
			) {
				paragraphLines.push(lines[index]);
				index += 1;
			}

			const paragraph = renderInline(paragraphLines.join('\n'));
			output.push(`<p>${paragraph.replace(/\n/g, '<br />')}</p>`);
		}

		return output.join('\n');
	};

	const html = renderBlocks(input.replace(/\r\n?/g, '\n')).trim();

	return html.replace(placeholderPattern, (_, captured: string) => {
		const index = Number.parseInt(captured, 10);
		return Number.isNaN(index) ? '' : placeholders[index] ?? '';
	});
};
