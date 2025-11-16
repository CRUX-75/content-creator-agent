// src/prompts/brand-identity.ts

export const MASTER_COPYWRITER_PROMPT = `
Du bist ein hochqualifizierter Copywriter-Agent für DOGONAUTS. Deine Aufgabe ist es,
präzise, markenkonforme Texte zu erzeugen, die informieren und zur Aktion führen.

AUSRICHTUNG (Ogilvy/Halbert/Schwartz):
- Klarheit & Wahrheit (keine Übertreibungen, Vertrauen aufbauen)
- Konkreter Nutzen & starke Value Proposition
- Eine große Idee pro Text
- Sprache der Zielgruppe
- Storytelling mit klarem, markenkonformem CTA

AUSGABEFORMAT:
- Du gibst AUSSCHLIESSLICH JSON zurück: { "headline": string, "caption": string }
- Kein Markdown, kein Fließtext außerhalb des JSON.

HARTE REGELN (müssen IMMER erfüllt werden):
- Sprache: Natürliches, flüssiges Deutsch (kein Übersetzungston).
- headline: MAXIMAL 40 Zeichen (nicht überschreiten).
- caption: MAXIMAL 150 Zeichen (inkl. Hashtags).
- Hashtags in der caption: insgesamt 2–4 Stück.
  - #dogonauts MUSS enthalten sein.
  - Weitere Hashtags müssen thematisch relevant sein (Kategorie/Marke/Produkt).

STILREGELN:
- clean: direkt, sachlich, KEINE Emojis.
- warm: empathisch, nahbar, KEINE Emojis.
- fun: verspielt, 0–1 Emoji erlaubt.
- tech: sachlich-präzise, KEINE Emojis.
- Verboten: DAUER-GROSSSCHRIFT, zu viele Ausrufezeichen, absolute Heilsversprechen.

VARIATION & WIEDERHOLUNGS-VERBOT:
- Der ERSTE Satz der caption muss sich deutlich von anderen Captions unterscheiden.
- Verwende NICHT die Formulierung "Kennst du das Gefühl" oder sehr ähnliche Varianten
  (z.B. "Kennst du dieses Gefühl", "Kennst du das, wenn ...").
- Vermeide es, mehrere Captions mit der gleichen Einstiegsstruktur zu beginnen.
  Wechsle bewusst zwischen:
  - direkter Frage,
  - klarer Aussage,
  - kurzer Alltagsszene mit Hund,
  - "Stell dir vor..."-Einstieg.
- Nutze die vom Nutzerprompt vorgegebene Hook-Variante als harte Leitlinie.

INTERNE VALIDIERUNG (vor der Ausgabe):
- Prüfe, ob headline ≤ 40 und caption ≤ 150.
- Prüfe, ob die Hashtags 2–4 sind und #dogonauts enthalten.
- Prüfe die Emoji-Regel je Stil.
- Bei Regelverstoß: intern korrigieren und ERST DANN das gültige JSON ausgeben.
`.trim();

export const DOGONAUTS_BRAND_IDENTITY = `
MARKENKONTEXT: DOGONAUTS
Premium-Hundebedarf aus Deutschland. E-Commerce für Premium-Futter & Zubehör
mit Raumfahrt-Anklang (maßvoll).

USPs:
- Bekannte Marken
- Faire Preise
- Schneller Versand
- Große Auswahl

Tonality:
- Klar, ehrlich, vertrauensbildend
- Nutzen im Fokus
- Weltraum-Metaphern sparsam und passend
- Keine Kosenamen/verniedlichenden Klischees
`.trim();

export const STYLE_MODIFIERS = {
  clean: {
    tone: "Minimalistisch, direkt, professionell",
    approach: `
- Eine zentrale Idee
- Kurze Sätze (≤ 8 Wörter)
- Nutzen und Effizienz betonen
    `,
    examples: {
      headline: "Premium. Fair. Schnell.",
      caption:
        "Royal Canin 15kg. In 24h bei dir. Faire Preise. #dogonauts #royalcanin",
    },
  },
  warm: {
    tone: "Warmherzig, verbindend, vertrauensvoll",
    approach: `
- Emotionale Verbindung Hund–Mensch
- Fürsorge & Vertrauen betonen
    `,
    examples: {
      headline: "Weil er es verdient",
      caption:
        "Hill's Sensitive. Tierärztlich empfohlen. Für deinen Liebling. #dogonauts #hillspet",
    },
  },
  fun: {
    tone: "Energisch, verspielt",
    approach: `
- Maximal 1 relevantes Emoji
- Leichtes Weltraum-Thema
    `,
    examples: {
      headline: "Bereit zum Abheben? 🚀",
      caption:
        "Kong Classic. Für Power-Kauer. Challenge accepted! #dogonauts #kong",
    },
  },
  tech: {
    tone: "Innovativ, Spezifikationen + Nutzen",
    approach: `
- Technisches Detail + klarer Vorteil
    `,
    examples: {
      headline: "Engineered for trails",
      caption:
        "Ruffwear Front Range. 5-Punkt-Verteiler, reflektierend. #dogonauts #ruffwear",
    },
  },
} as const;

export const HASHTAG_STRATEGY = {
  mandatory: ["#dogonauts"],
  categories: {
    futter: ["#hundefutter", "#dogfood", "#premiumfutter"],
    trockenfutter: ["#trockenfutter", "#dryfood"],
    nassfutter: ["#nassfutter", "#wetfood"],
    snacks: ["#hundesnacks", "#leckerlis", "#treats"],
    leash: ["#hundeleine", "#dogleash"],
    collar: ["#hundehalsband", "#dogcollar"],
    harness: ["#hundegeschirr", "#dogharness"],
    toy: ["#hundespielzeug", "#dogtoys"],
    bowl: ["#hundenapf", "#dogbowl"],
    care: ["#hundepflege", "#dogcare"],
  },
  brands: {
    "royal-canin": ["#royalcanin"],
    hills: ["#hillspet", "#scienceplan"],
    purina: ["#purina", "#proplan"],
    kong: ["#kong", "#kongtoy"],
    ruffwear: ["#ruffwear"],
    trixie: ["#trixie"],
  },
  styleHashtags: {
    clean: ["#hundebedarf", "#dogsupplies"],
    warm: ["#hundeliebe", "#doglove"],
    fun: ["#doglife", "#hundeleben"],
    tech: ["#premiumdog", "#qualität"],
  },
  reach: ["#dogstagram"], // optional, minimal
} as const;

// --- Helpers ---

function detectCategory(name: string, description?: string) {
  const text = `${name} ${description ?? ""}`.toLowerCase();
  if (/trockenfutter|dry food|kibble/.test(text)) return "trockenfutter";
  if (/nassfutter|wet food|dose/.test(text)) return "nassfutter";
  if (/snack|leckerli|treat/.test(text)) return "snacks";
  if (/futter|food/.test(text)) return "futter";
  if (/leine|leash/.test(text)) return "leash";
  if (/halsband|collar/.test(text)) return "collar";
  if (/geschirr|harness/.test(text)) return "harness";
  if (/spielzeug|toy/.test(text)) return "toy";
  if (/napf|bowl/.test(text)) return "bowl";
  return "accessory";
}

function detectBrand(name: string) {
  const list = ["royal-canin", "hills", "purina", "kong", "ruffwear", "trixie"];
  const s = name.toLowerCase();
  for (const b of list) {
    if (s.includes(b) || s.includes(b.replace("-", " "))) return b;
  }
  return null;
}

function extractFeatures(description?: string) {
  const out: string[] = [];
  const t = (description ?? "").toLowerCase();
  if (/getreidefrei|grain\-free/.test(t)) out.push("getreidefrei");
  if (/sensitiv|sensitive/.test(t)) out.push("für sensible Hunde");
  if (/welpe|puppy/.test(t)) out.push("für Welpen");
  if (/senior/.test(t)) out.push("für Senioren");
  if (/reflektierend|reflective/.test(t)) out.push("reflektierend");
  if (/wasserdicht|waterproof/.test(t)) out.push("wasserdicht");
  if (/gepolstert|padded/.test(t)) out.push("gepolstert");
  if (/verstellbar|adjustable/.test(t)) out.push("verstellbar");
  return out;
}

function classifyPrice(price?: number) {
  if (price == null) return "unknown";
  if (price < 20) return "budget-friendly";
  if (price < 50) return "mid-range";
  return "premium";
}

function unique<T>(arr: T[]) {
  return [...new Set(arr)];
}

function buildHashtagList(opts: {
  category: string;
  brand: string | null;
  style: string;
}) {
  const { category, brand, style } = opts;

  const catTags = (HASHTAG_STRATEGY.categories as any)[category] ?? [];
  const brandTags = brand
    ? ((HASHTAG_STRATEGY.brands as any)[brand] ?? [])
    : [];
  const styleTags =
    (HASHTAG_STRATEGY.styleHashtags as any)[style] ??
    (HASHTAG_STRATEGY.styleHashtags as any).clean;

  // Base: immer #dogonauts
  let list = [
    ...HASHTAG_STRATEGY.mandatory,
    ...catTags,
    ...brandTags,
    ...styleTags,
  ];

  list = unique(list).filter(Boolean);

  // Regeln: total 2–4 (inkl. #dogonauts)
  if (list.length > 4) list = list.slice(0, 4);
  if (list.length < 2) {
    const reach = HASHTAG_STRATEGY.reach[0];
    if (!list.includes(reach)) list.push(reach);
  }
  // Garantieren: #dogonauts
  if (!list.includes("#dogonauts")) list.unshift("#dogonauts");

  if (list.length > 4) list = list.slice(0, 4);

  return list;
}

export function buildProductContext(product: any, style?: string) {
  const category = detectCategory(product.name, product.description);
  const brand = detectBrand(product.name);
  const features = extractFeatures(product.description);
  const pricePosition = classifyPrice(product.price);
  const hashtags = buildHashtagList({
    category,
    brand,
    style: style ?? "clean",
  });

  return { category, brand, features, pricePosition, hashtags };
}

// --- Hook-Varianten für Variation im ersten Satz ---

const HOOK_MODES = [
  'Starte mit einer direkten, knappen Frage an den Leser (ohne "Kennst du das Gefühl" oder ähnliche Formulierungen).',
  "Starte mit einer kurzen Alltagsszene mit Hund (z.B. zu Hause, unterwegs oder im Urlaub).",
  "Starte mit einer klaren, starken Aussage über das Problem oder den Vorteil des Produkts.",
  'Starte mit einem "Stell dir vor ..."-Satz, der den Leser in eine Situation mit seinem Hund hineinzieht.',
] as const;

function pickHookMode() {
  const idx = Math.floor(Math.random() * HOOK_MODES.length);
  return HOOK_MODES[idx];
}

// ---- Prompt Builder ----

export function buildFinalPrompt(product: any, style: string) {
  const ctx = buildProductContext(product, style);
  const mod = (STYLE_MODIFIERS as any)[style] ?? STYLE_MODIFIERS.clean;

  const systemPrompt = `
${MASTER_COPYWRITER_PROMPT}

${DOGONAUTS_BRAND_IDENTITY}
`.trim();

  const suggestedHashtags = ctx.hashtags.join(" ");
  const hookInstruction = pickHookMode();

  const userPrompt = `
AUFGABE: Erzeuge "headline" und "caption" als JSON.

PRODUKT:
- Name: ${product.name}
- Beschreibung: ${product.description ?? "Premium Hundebedarf"}
- Preis: €${product.price ?? "—"}
- Kategorie: ${ctx.category}
- Marke: ${ctx.brand ?? "N/A"}
- Features: ${ctx.features.join(", ") || "N/A"}
- Preispositionierung: ${ctx.pricePosition}

STIL: ${style}
${mod.tone}

STIL-RICHTLINIEN:
${mod.approach}

HOOK-VARIANTE (MUSS beachtet werden):
${hookInstruction}

REFERENZ (${style}):
Headline: "${mod.examples.headline}"
Caption: "${mod.examples.caption}"

HASHTAG-VORGABE:
- Verwende am Ende der Caption GENAU 2–4 Hashtags aus dieser Liste (in beliebiger Reihenfolge),
  "#dogonauts" MUSS enthalten sein:
  ${suggestedHashtags}

HARTE PRÜFUNG:
- headline ≤ 40 Zeichen
- caption ≤ 150 Zeichen (inkl. Hashtags)
- Hashtags gesamt 2–4, inkl. #dogonauts
- Emojis: nur im Stil "fun", maximal 1
- Output: nur JSON { "headline": string, "caption": string }
- Bei Verstoß: intern korrigieren und erst dann ausgeben.
`.trim();

  return { systemPrompt, userPrompt };
}
