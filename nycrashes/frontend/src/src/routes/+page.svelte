<script lang="ts">
	import { afterUpdate, onMount } from 'svelte';
	import { fade } from 'svelte/transition';

	type Role = 'user' | 'assistant';

	interface Message {
		id: string;
		role: Role;
		text: string;
		isStreaming?: boolean;
		isError?: boolean;
	}

	interface Suggestion {
		title: string;
		prompt: string;
	}

	const suggestions: Suggestion[] = [
		{
			title: 'Crash hotspots',
			prompt: 'What are the current crash hotspots in Manhattan and how do they compare to last month?'
		},
		{
			title: 'Seasonal trends',
			prompt: 'Summarize how crash frequency changes by season across the five boroughs.'
		},
		{
			title: 'Vision Zero progress',
			prompt: 'Is Vision Zero improving safety in Queens? Highlight any notable intersections.'
		},
		{
			title: 'Weekday vs weekend',
			prompt: 'How do weekend crash patterns differ from weekdays, especially for cyclist incidents?'
		}
	];

	let messages: Message[] = [];
	let draft = '';
	let isSending = false;
	let bannerMessage: string | null = null;
	let chatScrollEl: HTMLDivElement | null = null;
	let inputEl: HTMLTextAreaElement | null = null;
	let autoScroll = true;
	let historyLoaded = false;

	const createId = () =>
		typeof crypto !== 'undefined' && 'randomUUID' in crypto
			? crypto.randomUUID()
			: `msg-${Date.now()}-${Math.random().toString(16).slice(2)}`;

	onMount(async () => {
		await preloadHistory();
		historyLoaded = true;
		focusInput();
	});

	async function preloadHistory() {
		try {
			const res = await fetch('/api/chat');
			if (!res.ok) {
				throw new Error(`Unable to load your recent conversation (status ${res.status}).`);
			}

			const payload = (await res.json()) as {
				messages?: { role?: string; content?: { text?: string }[] }[];
			};

			const restored = payload.messages
				?.map((message, index) => {
					if (!message.role || !message.content || message.content.length === 0) {
						return null;
					}

					const text = message.content[0]?.text ?? '';
					if (!text.trim()) {
						return null;
					}

					const role = message.role === 'assistant' ? 'assistant' : message.role === 'user' ? 'user' : null;
					if (!role) {
						return null;
					}

					return {
						id: `history-${index}`,
						role,
						text
					} satisfies Message;
				})
				.filter((message): message is Message => Boolean(message));

			if (restored?.length) {
				messages = restored;
			}
		} catch (error) {
			bannerMessage = getErrorMessage(error);
		}
	}

	function focusInput() {
		queueMicrotask(() => {
			inputEl?.focus();
		});
	}

	function pushMessage(message: Message) {
		messages = [...messages, message];
		autoScroll = true;
	}

	async function handleSend(promptOverride?: string) {
		const content = (promptOverride ?? draft).trim();
		if (!content || isSending) {
			return;
		}

		draft = '';
		bannerMessage = null;

		const userMessage: Message = {
			id: createId(),
			role: 'user',
			text: content
		};
		pushMessage(userMessage);

		const assistantMessage: Message = {
			id: createId(),
			role: 'assistant',
			text: '',
			isStreaming: true
		};
		pushMessage(assistantMessage);

		isSending = true;

		try {
			await streamAssistantResponse(content, assistantMessage);
		} catch (error) {
			const detail = getErrorMessage(error);
			assistantMessage.text = `I ran into an issue: ${detail}`;
			assistantMessage.isError = true;
			messages = [...messages];
			bannerMessage = detail;
		} finally {
			assistantMessage.isStreaming = false;
			messages = [...messages];
			isSending = false;
			focusInput();
		}
	}

	async function streamAssistantResponse(prompt: string, target: Message) {
		const response = await fetch('/api/chat', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json'
			},
			body: JSON.stringify({ prompt })
		});

		if (!response.ok) {
			const detail = await safeRead(response);
			throw new Error(detail ?? `Request failed with status ${response.status}`);
		}

		if (!response.body) {
			throw new Error('The chat service returned an empty response.');
		}

		const reader = response.body.getReader();
		const decoder = new TextDecoder('utf-8');
		let buffer = '';

		while (true) {
			const { value, done } = await reader.read();
			if (done) {
				buffer += decoder.decode();
				break;
			}

			buffer += decoder.decode(value, { stream: true });

			try {
				buffer = handleBuffer(buffer, target);
			} catch (error) {
				await reader.cancel();
				throw error;
			}
		}

		if (buffer.trim().length) {
			handleBuffer(buffer, target);
		}
	}

	function handleBuffer(buffer: string, target: Message): string {
		let remaining = buffer;
		let boundary = remaining.indexOf('\n\n');

		while (boundary !== -1) {
			const rawEvent = remaining.slice(0, boundary);
			remaining = remaining.slice(boundary + 2);
			const parsed = parseEvent(rawEvent);

			if (parsed?.event === 'error') {
				const detail =
					typeof parsed.data === 'string'
						? parsed.data
						: parsed?.data && typeof parsed.data === 'object' && 'error' in parsed.data
							? String((parsed.data as Record<string, unknown>).error)
							: 'Chat service returned an error.';
				throw new Error(detail);
			}

			const text = extractText(parsed?.data);
			if (text) {
				target.text += text;
				messages = [...messages];
				autoScroll = true;
			}

			boundary = remaining.indexOf('\n\n');
		}

		return remaining;
	}

	function parseEvent(raw: string): { event?: string; data?: unknown } | null {
		const lines = raw
			.split('\n')
			.map((line) => line.trim())
			.filter((line) => line.length);

		if (!lines.length) {
			return null;
		}

		let eventName: string | undefined;
		const dataLines: string[] = [];

		for (const line of lines) {
			if (line.startsWith('event:')) {
				eventName = line.slice(6).trim();
				continue;
			}

			if (line.startsWith('data:')) {
				dataLines.push(line.slice(5).trim());
			}
		}

		const dataPayload = dataLines.join('\n');
		if (!dataPayload) {
			return { event: eventName };
		}

		try {
			return { event: eventName, data: JSON.parse(dataPayload) };
		} catch {
			return { event: eventName, data: dataPayload };
		}
	}

	function extractText(payload: unknown): string {
		if (!payload) {
			return '';
		}

		if (typeof payload === 'string') {
			return payload;
		}

		if (Array.isArray(payload)) {
			return payload.map(extractText).join('');
		}

		if (typeof payload === 'object') {
			const candidates: Array<unknown> = [];
			const obj = payload as Record<string, unknown>;

			const directKeys = ['delta', 'text', 'content', 'output', 'value'];
			for (const key of directKeys) {
				if (typeof obj[key] === 'string') {
					return obj[key] as string;
				}
				if (obj[key]) {
					candidates.push(obj[key]);
				}
			}

			if (obj.message) {
				candidates.push(obj.message);
			}

			if (obj.data) {
				candidates.push(obj.data);
			}

			for (const candidate of candidates) {
				const nested = extractText(candidate);
				if (nested) {
					return nested;
				}
			}
		}

		return '';
	}

	async function safeRead(response: Response): Promise<string | null> {
		try {
			const value = await response.text();
			return value.trim() ? value.trim() : null;
		} catch {
			return null;
		}
	}

	function getErrorMessage(error: unknown): string {
		if (error instanceof Error) {
			return error.message;
		}

		if (typeof error === 'string') {
			return error;
		}

		return 'Something went wrong while talking to the crash insights service.';
	}

	function handleTextareaKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			handleSend();
		}
	}

	function handleScroll() {
		if (!chatScrollEl) {
			return;
		}

		const threshold = 80;
		const distanceFromBottom =
			chatScrollEl.scrollHeight - chatScrollEl.scrollTop - chatScrollEl.clientHeight;
		autoScroll = distanceFromBottom < threshold;
	}

	function bubbleClass(message: Message): string {
		if (message.role === 'user') {
			return 'rounded-3xl border border-sky-400/50 bg-gradient-to-br from-sky-500/90 to-sky-600/90 text-white shadow-lg shadow-sky-900/40';
		}

		if (message.isError) {
			return 'rounded-3xl border border-rose-500/50 bg-rose-500/10 text-rose-50';
		}

		return 'rounded-3xl border border-white/10 bg-slate-900/80 text-slate-100 backdrop-blur';
	}

	afterUpdate(() => {
		if (autoScroll && chatScrollEl) {
			chatScrollEl.scrollTo({
				top: chatScrollEl.scrollHeight,
				behavior: 'smooth'
			});
		}
	});
</script>

<div class="min-h-screen bg-gradient-to-b from-slate-950 via-slate-950/80 to-slate-950">
	<div class="relative mx-auto flex min-h-screen w-full max-w-5xl flex-col px-6 py-10 sm:py-14">
		<div class="pointer-events-none absolute inset-0 -z-10 opacity-70 blur-3xl">
			<div class="mx-auto h-full max-w-4xl rounded-full bg-gradient-to-r from-sky-500/20 via-cyan-400/10 to-emerald-400/10"></div>
		</div>

		<header class="space-y-6">
			<div class="inline-flex w-fit items-center gap-2 rounded-full border border-slate-800/60 bg-slate-900/70 px-4 py-2 text-xs font-semibold uppercase tracking-widest text-slate-300 shadow-sm shadow-slate-900/40">
				<span class="h-2 w-2 rounded-full bg-emerald-400 shadow-[0_0_12px_rgba(52,211,153,0.8)]"></span>
				Live NYC crash intelligence
			</div>

			<div class="space-y-3">
				<h1 class="text-3xl font-semibold text-white sm:text-4xl">
					Crash Safety Copilot
				</h1>
				<p class="max-w-3xl text-sm text-slate-300 sm:text-base">
					Investigate New York City crash data conversationally. Ask about corridor hot spots,
					compare borough performance, or drill into seasonal and modal trends to accelerate
					data-driven Vision Zero decisions.
				</p>
			</div>
		</header>

		{#if bannerMessage}
			<div class="mt-8 rounded-3xl border border-rose-500/40 bg-rose-500/10 px-5 py-4 text-sm text-rose-100 shadow-lg shadow-rose-950/40">
				<div class="flex items-start gap-3">
					<span class="mt-[2px] inline-flex h-5 w-5 flex-shrink-0 items-center justify-center rounded-full bg-rose-500/30 text-xs font-bold text-rose-100">!</span>
					<div>
						<p>{bannerMessage}</p>
						<p class="mt-2 text-xs text-rose-200/70">Please try again, or check that the analytics service is reachable.</p>
					</div>
				</div>
			</div>
		{/if}

		<section class="mt-8 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-xs font-semibold uppercase tracking-[0.35em] text-slate-400">
					Suggested prompts
				</h2>
				{#if messages.length}
					<button
						type="button"
						class="text-xs font-medium text-slate-400 underline-offset-4 transition hover:text-sky-300 hover:underline"
						on:click={() => {
							messages = [];
							autoScroll = true;
							focusInput();
						}}
					>
						Clear conversation
					</button>
				{/if}
			</div>

			<div class="flex flex-wrap gap-3">
				{#each suggestions as suggestion, index}
					<button
						type="button"
						class="group inline-flex max-w-[20rem] flex-1 items-start gap-3 rounded-3xl border border-white/10 bg-slate-900/60 px-4 py-3 text-left text-sm text-slate-200 shadow-lg shadow-slate-950/40 transition hover:border-sky-400/60 hover:bg-slate-900/90 hover:text-white focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-400 disabled:opacity-70"
						on:click={() => handleSend(suggestion.prompt)}
						disabled={isSending}
					>
						<span class="mt-0.5 inline-flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-sky-500/15 text-[11px] font-semibold text-sky-300 transition group-hover:bg-sky-400/40 group-hover:text-white">
							{index + 1}
						</span>
						<span>
							<span class="block font-medium text-slate-100 group-hover:text-white">{suggestion.title}</span>
							<span class="mt-1 block text-xs text-slate-400 group-hover:text-slate-200">{suggestion.prompt}</span>
						</span>
					</button>
				{/each}
			</div>
		</section>

		<section class="mt-8 flex-1 overflow-hidden rounded-[2.25rem] border border-white/10 bg-slate-950/70 shadow-[0_40px_120px_-50px_rgba(14,165,233,0.45)] backdrop-blur">
			<div class="flex h-full flex-col">
				<div
					class="flex-1 overflow-y-auto px-6 py-8 sm:px-8 sm:py-10"
					bind:this={chatScrollEl}
					on:scroll={handleScroll}
				>
					{#if !messages.length && historyLoaded}
						<div class="grid h-full place-items-center text-center text-sm text-slate-400">
							<div class="max-w-lg space-y-4">
								<h3 class="text-lg font-semibold text-slate-200">How can I help?</h3>
								<p class="leading-relaxed text-slate-400">
									Start with one of the prompts above or ask anything about New York City motor
									vehicle crashes&mdash;borough comparisons, modal risk, time-of-day patterns, and
									more.
								</p>
							</div>
						</div>
					{:else if !historyLoaded}
						<div class="grid h-full place-items-center">
							<div class="h-12 w-12 animate-spin rounded-full border-2 border-slate-700 border-t-sky-400"></div>
						</div>
					{:else}
						<div class="flex flex-col gap-6">
							{#each messages as message (message.id)}
								<div class={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`} transition:fade={{ duration: 150 }}>
									<div class="max-w-[78%] space-y-3 text-sm leading-relaxed">
										<div class={bubbleClass(message)}>
											<div class="flex items-center gap-3 text-xs font-semibold uppercase tracking-widest">
												<span
													class={
														message.role === 'user'
															? 'text-white/90'
															: message.isError
																? 'text-rose-200'
																: 'text-sky-300'
													}
												>
													{message.role === 'user' ? 'You' : 'Crash Copilot'}
												</span>

												{#if message.isStreaming}
													<span class="inline-flex items-center gap-2 rounded-full bg-sky-500/10 px-2 py-1 text-[10px] font-medium uppercase tracking-[0.35em] text-sky-300">
														<span class="h-1.5 w-1.5 animate-pulse rounded-full bg-sky-400"></span>
														Drafting
													</span>
												{/if}
											</div>
											<p class="mt-3 whitespace-pre-wrap text-sm leading-relaxed text-slate-100 [word-break:break-word]">
												{message.text || (message.isStreaming ? '...' : '')}
											</p>
										</div>
									</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>

				<form
					class="border-t border-white/10 bg-slate-950/80 px-6 py-6 sm:px-8"
					on:submit|preventDefault={() => handleSend()}
				>
					<div class="flex items-end gap-4">
						<div class="relative flex-1">
							<textarea
								bind:this={inputEl}
								bind:value={draft}
								class="max-h-40 min-h-[3.25rem] w-full resize-none rounded-3xl border border-white/10 bg-slate-900/80 px-5 py-4 text-sm leading-relaxed text-slate-100 shadow-inner shadow-slate-950/60 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-400/40"
								placeholder="Ask about crash hotspots, modal risk, or borough comparisons..."
								rows={1}
								on:keydown={handleTextareaKeydown}
							></textarea>
							<div class="pointer-events-none absolute inset-x-5 bottom-2 text-[10px] uppercase tracking-[0.4em] text-slate-500">
								Enter to send · Shift + Enter for new line
							</div>
						</div>

						<button
							type="submit"
							class="inline-flex h-12 w-12 items-center justify-center rounded-3xl bg-sky-500 text-white shadow-[0_10px_35px_-10px_rgba(14,165,233,0.75)] transition hover:bg-sky-400 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-sky-300 disabled:cursor-not-allowed disabled:bg-sky-500/40 disabled:text-slate-300"
							disabled={isSending || !draft.trim()}
							title="Send message"
						>
							<span class="sr-only">Send</span>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								viewBox="0 0 20 20"
								fill="currentColor"
								class="h-5 w-5"
							>
								<path
									fill-rule="evenodd"
									d="M3.22 2.22a.75.75 0 01.79-.18l13 5a.75.75 0 010 1.38l-5.2 2.08a.25.25 0 00-.16.2l-.39 2.7a.75.75 0 01-1.18.5l-2.54-1.9a.25.25 0 00-.2-.05l-2.73.55a.75.75 0 01-.87-.97l3-9a.75.75 0 01.18-.3z"
									clip-rule="evenodd"
								/>
							</svg>
						</button>
					</div>
				</form>
			</div>
		</section>
	</div>
</div>
