// src/jobs/publishPost.ts
import { supabase } from "../db/supabase.js";
import fetch from "node-fetch";

const META_GRAPH_VERSION =
  process.env.META_GRAPH_VERSION || "v21.0";
const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN!;
const IG_ACCOUNT_ID = process.env.IG_ACCOUNT_ID!;
const META_PUBLISH_DRY_RUN =
  (process.env.META_PUBLISH_DRY_RUN || "false").toLowerCase() === "true";

export async function runPublishPostPipeline(generatedPostId: string) {
  // 1) Cargar post
  const { data: post, error: postError } = await supabase
    .from("generated_posts")
    .select(
      `
      id,
      product_id,
      caption_ig,
      status,
      channel_target,
      style,
      meta_post_id,
      channel_published
    `
    )
    .eq("id", generatedPostId)
    .single();

  if (postError || !post) {
    throw new Error(`generated_post no encontrado: ${postError?.message}`);
  }

  if (post.status !== "DRAFT") {
    throw new Error(
      `Solo se pueden publicar DRAFTs; estado actual=${post.status}`
    );
  }

  // 2) Obtener URL de imagen y URL de producto
  const { data: product, error: productError } = await supabase
    .from("products")
    .select("id, product_name, image_url, bild2, bild3, bild4, bild5, bild6, bild7, bild8, url")
    .eq("id", post.product_id)
    .single();

  if (productError || !product) {
    throw new Error(`Producto no encontrado: ${productError?.message}`);
  }

  const imageUrl =
    normalizeImageUrl(
      product.image_url ||
        product.bild2 ||
        product.bild3 ||
        product.bild4 ||
        product.bild5 ||
        product.bild6 ||
        product.bild7 ||
        product.bild8
    );

  if (!imageUrl) {
    throw new Error(`Producto ${product.id} no tiene imagen válida`);
  }

  const productUrl = product.url; // ajusta al campo real con la URL del producto
  const finalCaption = buildFinalCaption(post.caption_ig, productUrl);

  // 3) Meta Graph API (IG): crear media container
  let igMediaId: string | null = null;
  const channel = "IG"; // por ahora solo IG

  if (!META_PUBLISH_DRY_RUN) {
    // create media container
    const createRes = await fetch(
      `https://graph.facebook.com/${META_GRAPH_VERSION}/${IG_ACCOUNT_ID}/media`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          image_url: imageUrl,
          caption: finalCaption,
          access_token: META_ACCESS_TOKEN,
        }),
      }
    );

    const createJson: any = await createRes.json();

    if (!createRes.ok || !createJson.id) {
      throw new Error(
        `Error creando media: ${createRes.status} - ${JSON.stringify(
          createJson
        )}`
      );
    }

    const creationId = createJson.id as string;

    // publish media
    const publishRes = await fetch(
      `https://graph.facebook.com/${META_GRAPH_VERSION}/${IG_ACCOUNT_ID}/media_publish`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          creation_id: creationId,
          access_token: META_ACCESS_TOKEN,
        }),
      }
    );

    const publishJson: any = await publishRes.json();

    if (!publishRes.ok || !publishJson.id) {
      throw new Error(
        `Error publicando media: ${publishRes.status} - ${JSON.stringify(
          publishJson
        )}`
      );
    }

    igMediaId = publishJson.id as string;
  } else {
    // DRY-RUN: simular media_id
    igMediaId = `dryrun_${generatedPostId}`;
  }

  // 4) Actualizar generated_posts → PUBLISHED + canal
  const { error: updateError } = await supabase
    .from("generated_posts")
    .update({
      status: "PUBLISHED",
      channel_published: channel,
      meta_post_id: igMediaId, // ya lo tenías en el esquema
      published_at: new Date().toISOString(),
    })
    .eq("id", generatedPostId);

  if (updateError) {
    throw new Error(
      `Error actualizando generated_posts: ${updateError.message}`
    );
  }

  // 5) Semilla en post_feedback
  const { error: feedbackError } = await supabase
    .from("post_feedback")
    .insert({
      generated_post_id: generatedPostId,
      channel,
      ig_media_id: igMediaId!,
      metrics: {}, // vacío, se llenará el worker
      collected_at: null,
    });

  if (feedbackError) {
    // no tiramos el job, pero lo logueamos fuerte
    console.error("Error insertando post_feedback:", feedbackError.message);
  }

  return { igMediaId, channel };
}

function buildFinalCaption(caption: string, productUrl?: string | null) {
  if (!productUrl) return caption;
  return `${caption}\n\n🔗 Shop it here: ${productUrl}`;
}

function normalizeImageUrl(raw?: string | null): string | null {
  if (!raw) return null;
  let url = raw.trim();
  if (!/^https?:\/\//i.test(url)) {
    // arreglar URLs tipo //cdn...
    url = "https:" + (url.startsWith("//") ? url : "//" + url);
  }
  return url;
}
