import type {
  ExtractInput,
  ExtractResponse,
  ExtractedProduct,
  ExtractedVariant,
  ExtractedVariantRow,
  Extractor,
  StockStatus,
} from "./types";

const OFFICIAL_DESC_TEMPLATES = [
  "Experience the ultimate luxury with {title}. Formulated with rare exotic ingredients to deliver unparalleled results.",
  "The iconic {title} defines modern sophistication. A proprietary blend that enhances natural beauty.",
  "Discover {title}, a masterpiece of craftsmanship. Designed for the discerning individual who accepts no compromises.",
  "{title} offers a transformative experience. Its advanced formula provides long-lasting perfection.",
  "Unleash your potential with {title}. A cult classic reimagined for the contemporary era.",
];

const SOCIAL_CONTENT_TEMPLATES = [
  "Trending on TikTok: 'The finish is absolutely unreal.' Users report all-day wear without touch-ups.",
  "Instagram favorite: Influencers are obsessed with the {variant} shade. 'My new holy grail,' says @BeautyGuru.",
  "Viral hit: This specific {variant} is selling out everywhere. 'Worth every penny for the glow alone.'",
  "Community top pick: 4.8/5 stars on social platforms. Fans love how it feels weightless yet powerful.",
  "As seen on #BeautyTok: 'Best investment for your routine.' The hype around {variant} is real.",
];

const AD_SUBJECT_TEMPLATES = [
  "✨ Back in Stock: {title} in {variant}",
  "Why everyone is talking about {title} ({variant})",
  "Your new obsession: {title}",
  "Exclusive: The perfect {variant} shade is here",
  "Luxury Redefined: Meet {title}",
];

const AD_CAPTION_TEMPLATES = [
  "Finally got my hands on {title} in {variant} and I'm obsessed! 😍 The texture is incredible and it lasts all day. \n\n#TomFordBeauty #LuxuryMakeup #BeautyFaves #{variant}",
  "Pov: You found the perfect {variant} shade. ✨ {title} is worth the hype. Tap the link to shop before it sells out! \n\n#MakeupAddict #SplurgeWorthy #{variant} #TomFord",
  "Elevate your routine with {title}. The shade {variant} is absolute perfection for any occasion. 🖤 \n\n#BeautyEssentials #LuxuryLife #{variant}",
  "Run don't walk! 🏃‍♀️ {title} in {variant} is the viral product of the season. \n\n#ViralBeauty #TomFord #{variant} #MakeupHaul",
];

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function getMergedDescription(title: string, variantValue: string) {
  const official = pick(OFFICIAL_DESC_TEMPLATES).replace("{title}", title);
  const social = pick(SOCIAL_CONTENT_TEMPLATES).replace("{variant}", variantValue);
  return `OFFICIAL: ${official} /// SOCIAL HIGHLIGHTS: ${social}`;
}

function generateMockAdCopy(title: string, variantValue: string, price: string) {
  const subject = pick(AD_SUBJECT_TEMPLATES).replace("{title}", title).replace("{variant}", variantValue);
  const caption = pick(AD_CAPTION_TEMPLATES).replace("{title}", title).replace("{variant}", variantValue);
  return `**Subject:** ${subject}\n\n**Instagram Caption:**\n${caption}\n\n**Price:** $${price}`;
}

function getImageUrl(sku: string) {
  return `https://www.tomfordbeauty.com/media/export/cms/products/1000x1000/${sku}.jpg`;
}

const KNOWN_URLS: Record<string, string> = {
  "Figue Érotique Eau de Parfum": "https://www.tomfordbeauty.com/product/figue-erotique-eau-de-parfum",
  "Architecture Soft Matte Blurring Foundation":
    "https://www.tomfordbeauty.com/product/architecture-radiance-hydrating-foundation-broad-spectrum-spf-50",
  "Slim Lip Color Shine": "https://www.tomfordbeauty.com/product/slim-lip-color-shine",
};

function getSafeProductUrl(title: string) {
  if (KNOWN_URLS[title]) return KNOWN_URLS[title];

  const t = title.toLowerCase();
  if (t.includes("parfum") || t.includes("orchid") || t.includes("vetiver") || t.includes("soleil")) {
    return "https://www.tomfordbeauty.com/fragrance";
  }
  if (t.includes("lip") || t.includes("gloss") || t.includes("lacquer")) {
    return "https://www.tomfordbeauty.com/makeup/lips";
  }
  if (t.includes("foundation") || t.includes("concealer") || t.includes("bronzer") || t.includes("shade")) {
    return "https://www.tomfordbeauty.com/makeup/face";
  }
  if (t.includes("eye") || t.includes("brow") || t.includes("quad")) {
    return "https://www.tomfordbeauty.com/makeup/eyes";
  }
  return "https://www.tomfordbeauty.com/makeup";
}

const SIZES = ["30ml", "50ml", "100ml", "250ml Decanter"] as const;
const PRIVATE_BLEND_FRAGRANCES = [
  "Oud Wood",
  "Tobacco Vanille",
  "Lost Cherry",
  "Bitter Peach",
  "Fucking Fabulous",
  "Neroli Portofino",
  "Soleil Blanc",
  "Tuscan Leather",
  "Rose Prick",
  "Jasmine Rouge",
  "White Suede",
  "Vanille Fatale",
  "Ébène Fumé",
  "Santal Blush",
  "Soleil Neige",
  "Mandarino Di Amalfi",
  "Lavender Extreme",
  "Tubéreuse Nue",
  "Rose D'Amalfi",
  "Bois Marocain",
] as const;

const SIGNATURE_FRAGRANCES = [
  "Black Orchid",
  "Velvet Orchid",
  "Noir Extreme",
  "Grey Vetiver",
  "Beau de Jour",
  "Ombré Leather",
  "Costa Azzurra",
  "Noir",
  "Orchid Soleil",
  "Metallique",
] as const;

const FOUNDATION_SHADES = [
  "0.0 Pearl",
  "0.1 Cameo",
  "0.3 Ivory Silk",
  "0.4 Rose",
  "0.5 Porcelain",
  "1.1 Warm Sand",
  "1.3 Nude Ivory",
  "1.4 Bone",
  "1.5 Cream",
  "2.0 Buff",
  "2.5 Linen",
  "2.7 Vellum",
  "3.5 Ivory Rose",
  "3.7 Champagne",
  "4.0 Fawn",
  "4.5 Ivory",
  "4.7 Cool Beige",
  "5.1 Cool Almond",
  "5.5 Bisque",
  "5.6 Ivory Beige",
  "5.7 Dune",
  "6.0 Natural",
  "6.5 Sable",
  "7.0 Tawny",
  "7.5 Shell Beige",
  "7.7 Honey",
  "8.0 Praline",
  "8.2 Warm Honey",
  "8.7 Golden Almond",
  "9.0 Sienna",
  "9.5 Warm Almond",
  "9.7 Cool Honey",
  "10.0 Chestnut",
  "10.5 Mocha",
  "11.0 Dusk",
  "11.5 Warm Espresso",
  "12.0 Espresso",
  "12.5 Cool Espresso",
  "13.0 Black",
] as const;

const CONCEALER_SHADES = FOUNDATION_SHADES.slice(0, 25);

const LIP_COLORS = [
  "01 Insatiable",
  "02 Libertine",
  "03 West Coast",
  "04 Indian Rose",
  "06 Flame",
  "07 Ruby Rush",
  "08 Velvet Cherry",
  "09 True Coral",
  "10 Cherry Lush",
  "15 Wild Ginger",
  "16 Scarlet Rouge",
  "22 Forbidden Pink",
  "31 Twist of Fate",
  "44 Sunset Blvd",
  "69 Night Mauve",
  "80 Impassioned",
  "100 Equus",
  "100 L'Amant",
  "151 Iconic Nude",
  "152 Rose",
  "303 Empire",
  "510 Fascinator",
  "511 Steel Magnolia",
  "01 Spanish Pink",
  "03 Casablanca",
  "04 Pussycat",
  "05 Plum Lush",
  "06 Wicked Ways",
  "07 Ruby Rush",
] as const;

const EYE_QUADS = [
  "01 Golden Mink",
  "03 Nude Dip",
  "04 Honeymoon",
  "20 Disco Dust",
  "23 African Violet",
  "25 Pretty Baby",
  "26 Leopard Sun",
  "27 Virgin Orchid",
  "28 De La Crème",
  "29 Desert Fox",
  "30 Insolent Rose",
  "31 Sous Le Sable",
  "35 Rose Topaz",
  "36 Tiger Eye",
  "37 Smoky Quartz",
  "01 Insolent Rose",
  "02 Chalcedony",
  "03 Body Heat",
  "04 Suspicion",
  "05 Double Indemnity",
] as const;

function randomStock(): StockStatus {
  if (Math.random() > 0.95) return "Out of Stock";
  if (Math.random() > 0.8) return "Low Stock";
  return "In Stock";
}

function createVariantIdFactory(startAt = 100_000) {
  let current = startAt;
  return () => String(current++);
}

function createVariants(params: {
  title: string;
  baseSku: string;
  type: string;
  options: readonly string[];
  price: string;
  nextId: () => string;
  url?: string;
  imageUrlForSku?: (sku: string) => string;
}): ExtractedVariant[] {
  const baseUrl = params.url ?? getSafeProductUrl(params.title);
  return params.options.map((opt, i) => {
    const sku = `${params.baseSku}-${String(i).padStart(3, "0")}`;
    return {
      id: params.nextId(),
      sku,
      url: baseUrl,
      option_name: params.type,
      option_value: opt,
      price: params.price,
      currency: "USD",
      stock: randomStock(),
      description: getMergedDescription(params.title, opt),
      image_url: params.imageUrlForSku ? params.imageUrlForSku(sku) : getImageUrl(sku),
      ad_copy: generateMockAdCopy(params.title, opt, params.price),
    };
  });
}

function buildTomFordCatalog(targetVariants: number) {
  const nextId = createVariantIdFactory(100_000);
  const catalog: ExtractedProduct[] = [];

  // Worked example first
  catalog.push({
    title: "Figue Érotique Eau de Parfum",
    url: KNOWN_URLS["Figue Érotique Eau de Parfum"],
    variants: SIZES.map((size, idx) => {
      let price = "185.00";
      if (size === "50ml") price = "295.00";
      if (size === "100ml") price = "425.00";
      const sku = `TFB-FIGUE-${String(idx).padStart(2, "0")}`;
      return {
        id: String(900_000 + idx),
        sku,
        url: KNOWN_URLS["Figue Érotique Eau de Parfum"],
        option_name: "Size",
        option_value: size,
        price,
        currency: "USD",
        stock: "In Stock",
        description: getMergedDescription("Figue Érotique Eau de Parfum", size),
        image_url: getImageUrl(sku),
        ad_copy: generateMockAdCopy("Figue Érotique Eau de Parfum", size, price),
      };
    }),
  });

  // Private Blend (Expensive)
  PRIVATE_BLEND_FRAGRANCES.forEach((frag, i) => {
    const title = `${frag} Eau de Parfum`;
    const baseUrl = getSafeProductUrl(title);
    catalog.push({
      title,
      url: baseUrl,
      variants: SIZES.map((size, idx) => {
        let price = "185.00";
        if (size === "50ml") price = "295.00";
        if (size === "100ml") price = "425.00";
        if (size.includes("250ml")) price = "850.00";
        const sku = `TFB-PB-${String(i).padStart(2, "0")}-${idx}`;
        return {
          id: String(200_000 + i * 100 + idx),
          sku,
          url: baseUrl,
          option_name: "Size",
          option_value: size,
          price,
          currency: "USD",
          stock: "In Stock",
          description: getMergedDescription(title, size),
          image_url: getImageUrl(sku),
          ad_copy: generateMockAdCopy(title, size, price),
        };
      }),
    });
  });

  // Signature Collection (Mid-Range)
  SIGNATURE_FRAGRANCES.forEach((frag, i) => {
    const title = `${frag} Eau de Parfum`;
    const baseUrl = getSafeProductUrl(title);
    catalog.push({
      title,
      url: baseUrl,
      variants: (["50ml", "100ml"] as const).map((size, idx) => {
        const price = size === "50ml" ? "150.00" : "210.00";
        const sku = `TFB-SIG-${String(i).padStart(2, "0")}-${idx}`;
        return {
          id: String(300_000 + i * 100 + idx),
          sku,
          url: baseUrl,
          option_name: "Size",
          option_value: size,
          price,
          currency: "USD",
          stock: "In Stock",
          description: getMergedDescription(title, size),
          image_url: getImageUrl(sku),
          ad_copy: generateMockAdCopy(title, size, price),
        };
      }),
    });
  });

  // Face
  catalog.push({
    title: "Traceless Soft Matte Foundation",
    url: getSafeProductUrl("Traceless Soft Matte Foundation"),
    variants: createVariants({
      title: "Traceless Soft Matte Foundation",
      baseSku: "TFB-FDN-MATTE",
      type: "Shade",
      options: FOUNDATION_SHADES,
      price: "90.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Architecture Soft Matte Blurring Foundation",
    url: getSafeProductUrl("Architecture Soft Matte Blurring Foundation"),
    variants: createVariants({
      title: "Architecture Soft Matte Blurring Foundation",
      baseSku: "TFB-FDN-ARCH",
      type: "Shade",
      options: FOUNDATION_SHADES,
      price: "150.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Traceless Stick Foundation",
    url: getSafeProductUrl("Traceless Stick Foundation"),
    variants: createVariants({
      title: "Traceless Stick Foundation",
      baseSku: "TFB-FDN-STICK",
      type: "Shade",
      options: FOUNDATION_SHADES.slice(0, 30),
      price: "90.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Emotionproof Concealer",
    url: getSafeProductUrl("Emotionproof Concealer"),
    variants: createVariants({
      title: "Emotionproof Concealer",
      baseSku: "TFB-CONC-EP",
      type: "Shade",
      options: CONCEALER_SHADES,
      price: "58.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Soleil Glow Bronzer",
    url: getSafeProductUrl("Soleil Glow Bronzer"),
    variants: createVariants({
      title: "Soleil Glow Bronzer",
      baseSku: "TFB-BRONZE",
      type: "Color",
      options: ["01 Gold Dust", "02 Terra", "03 Bronze Age"],
      price: "75.00",
      nextId,
    }),
  });

  // Lips
  catalog.push({
    title: "Lip Color",
    url: getSafeProductUrl("Lip Color"),
    variants: createVariants({
      title: "Lip Color",
      baseSku: "TFB-LIP-CREME",
      type: "Color",
      options: LIP_COLORS,
      price: "62.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Slim Lip Color Shine",
    url: getSafeProductUrl("Slim Lip Color Shine"),
    variants: createVariants({
      title: "Slim Lip Color Shine",
      baseSku: "TFB-LIP-SLIM",
      type: "Color",
      options: LIP_COLORS.slice(0, 20),
      price: "62.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Gloss Luxe",
    url: getSafeProductUrl("Gloss Luxe"),
    variants: createVariants({
      title: "Gloss Luxe",
      baseSku: "TFB-GLOSS",
      type: "Color",
      options: ["01 Disclosure", "04 Exquise", "08 Inhibition", "15 Frantic", "20 Phantome"],
      price: "62.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Lip Lacquer Luxe - Vinyl",
    url: getSafeProductUrl("Lip Lacquer Luxe - Vinyl"),
    variants: createVariants({
      title: "Lip Lacquer Luxe - Vinyl",
      baseSku: "TFB-LACQ-VINYL",
      type: "Color",
      options: LIP_COLORS.slice(5, 15),
      price: "62.00",
      nextId,
    }),
  });

  // Eyes
  catalog.push({
    title: "Eye Color Quad",
    url: getSafeProductUrl("Eye Color Quad"),
    variants: createVariants({
      title: "Eye Color Quad",
      baseSku: "TFB-EYE-QUAD",
      type: "Palette",
      options: EYE_QUADS,
      price: "90.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Eye Defining Pen",
    url: getSafeProductUrl("Eye Defining Pen"),
    variants: createVariants({
      title: "Eye Defining Pen",
      baseSku: "TFB-EYE-PEN",
      type: "Color",
      options: ["01 Deeper"],
      price: "62.00",
      nextId,
    }),
  });

  catalog.push({
    title: "Brow Sculptor",
    url: getSafeProductUrl("Brow Sculptor"),
    variants: createVariants({
      title: "Brow Sculptor",
      baseSku: "TFB-BROW",
      type: "Shade",
      options: ["01 Blonde", "02 Taupe", "03 Chestnut", "04 Espresso"],
      price: "54.00",
      nextId,
    }),
  });

  const countVariants = () => catalog.reduce((acc, p) => acc + p.variants.length, 0);
  const baseCount = countVariants();
  if (baseCount >= targetVariants) return { catalog, baseCount };

  // Pad the catalog to satisfy "massive data" simulation needs.
  const optionSets: Array<{ type: string; options: readonly string[]; price: string; prefix: string }> = [
    { type: "Shade", options: FOUNDATION_SHADES, price: "90.00", prefix: "TFB-SIM-FDN" },
    { type: "Color", options: LIP_COLORS, price: "62.00", prefix: "TFB-SIM-LIP" },
    { type: "Palette", options: EYE_QUADS, price: "90.00", prefix: "TFB-SIM-EYE" },
    { type: "Size", options: SIZES as unknown as readonly string[], price: "185.00", prefix: "TFB-SIM-PB" },
  ];

  let n = 0;
  while (countVariants() < targetVariants) {
    const set = pick(optionSets);
    const title = `Tom Ford Beauty - Limited Edition Drop ${String(n + 1).padStart(3, "0")}`;
    const baseSku = `${set.prefix}-${String(n).padStart(4, "0")}`;

    catalog.push({
      title,
      url: getSafeProductUrl(title),
      variants: createVariants({
        title,
        baseSku,
        type: set.type,
        options: set.options,
        price: set.price,
        nextId,
      }),
    });
    n++;
  }

  return { catalog, baseCount };
}

function computePricingStats(variants: ExtractedVariantRow[]) {
  const prices = variants
    .map((v) => Number.parseFloat(v.price))
    .filter((n) => Number.isFinite(n));

  if (prices.length === 0) return { currency: "USD" as const, min: 0, max: 0, avg: 0 };

  const min = Math.min(...prices);
  const max = Math.max(...prices);
  const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
  return { currency: "USD" as const, min, max, avg: Number(avg.toFixed(2)) };
}

export class SimulationExtractor implements Extractor {
  async extract(input: ExtractInput): Promise<ExtractResponse> {
    const now = new Date();
    const at = now.toISOString();

    const target = Number(process.env.SIMULATION_TARGET_VARIANTS) || 850;
    const brand = input.brand;
    const domain = input.domain;

    const logs: ExtractResponse["logs"] = [];
    const pushLog = (type: ExtractResponse["logs"][number]["type"], msg: string) => {
      logs.push({ at: new Date().toISOString(), type, msg });
    };

    pushLog("info", `Initializing Pivota Extraction for: ${brand}`);
    pushLog("info", `Checking connectivity to ${domain}...`);
    pushLog("info", "GET /products.json?limit=1");

    const isTomFord = brand.toLowerCase().includes("tom ford");
    let products: ExtractedProduct[] = [];
    let platform = "Unknown";
    let sitemap: string | undefined;

    if (isTomFord) {
      pushLog("warn", "HTTP 404 - Shopify feed not found.");
      pushLog("info", "Switching to Sitemap Discovery Strategy.");
      pushLog("success", "GET /robots.txt");
      pushLog("data", `> Found \"Sitemap: ${domain}/sitemap_index.xml\"`);
      pushLog("info", "Parsing Sitemap Index...");

      const built = buildTomFordCatalog(target);
      products = built.catalog;
      platform = "Custom / SFCC";
      sitemap = "https://www.tomfordbeauty.com/sitemap.xml";

      pushLog("data", "> Found product sitemaps: /sitemap_products_1.xml, /sitemap_products_2.xml");
      pushLog("info", "Extracting product URLs...");
      pushLog("success", `> Identified ${products.length} unique product parent URLs`);
      if (built.baseCount < target) {
        pushLog("warn", `Simulation padding enabled: base=${built.baseCount} variants -> target=${target} variants`);
      }
    } else {
      pushLog("success", "HTTP 200 - Shopify feed detected!");
      pushLog("data", "> Found 150 products in JSON feed");

      // Simple generic simulation: generate 20 products × 10 variants = 200 rows.
      const nextId = createVariantIdFactory(500_000);
      const baseTitle = brand || "Brand";
      const genericProducts: ExtractedProduct[] = [];
      for (let i = 0; i < 20; i++) {
        const title = `${baseTitle} Product ${String(i + 1).padStart(3, "0")}`;
        const productUrl = `https://${domain}/products/${encodeURIComponent(title.toLowerCase().replace(/\s+/g, "-"))}`;
        const options = Array.from({ length: 10 }, (_, idx) => `Variant ${idx + 1}`);
        genericProducts.push({
          title,
          url: productUrl,
          variants: createVariants({
            title,
            baseSku: `SIM-${String(i).padStart(3, "0")}`,
            type: "Variant",
            options,
            price: String((20 + (i % 10) * 2).toFixed(2)),
            nextId,
            url: productUrl,
            imageUrlForSku: (sku) => `https://via.placeholder.com/1000x1000/e5e7eb/9ca3af?text=${encodeURIComponent(sku)}`,
          }),
        });
      }
      products = genericProducts;
    }

    pushLog("info", "Beginning Variant Extraction (Enriching Data)...");

    const variants: ExtractedVariantRow[] = [];
    const adCopyById: Record<string, string> = {};

    for (const product of products) {
      for (const variant of product.variants) {
        const deepLink = `${variant.url}?variant=${variant.id}&utm_source=pivota&utm_medium=affiliate`;
        const simulated = !KNOWN_URLS[product.title];
        const row: ExtractedVariantRow = {
          ...variant,
          brand,
          product_title: product.title,
          product_url: product.url,
          deep_link: deepLink,
          simulated,
        };
        variants.push(row);
        adCopyById[variant.id] = variant.ad_copy;
      }
    }

    const pricing = computePricingStats(variants);
    pushLog("success", `Extraction Complete. ${variants.length} variants processed successfully.`);
    pushLog("info", "Ready for CSV Export (Contains Images & Descriptions & Ad Copy).");

    return {
      brand,
      domain,
      generated_at: at,
      mode: "simulation",
      platform,
      sitemap,
      products,
      variants,
      pricing,
      ad_copy: { by_variant_id: adCopyById },
      logs,
    };
  }
}
