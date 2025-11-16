// src/jobs/feedbackCollect.ts
import { supabase } from "../db/supabase.js";
import fetch from "node-fetch";

const META_GRAPH_VERSION =
  process.env.META_GRAPH_VERSION || "v21.0";
const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN!;

// peso simple: likes + 2 * comments
function computePerfScore(likes: number, comments: number) {
  return likes + 2 * comments;
}

export async function runFeedbackCollectJob() {
  // 1) Buscar posts pendientes de métricas (o con métricas antiguas)
  const { data: pending, error } = await supabase
    .from("post_feedback")
    .select(
      `
      id,
      generated_post_id,
      channel,
      ig_media_id,
      metrics,
      collected_at
    `
    )
    .eq("channel", "IG")
    .is("collected_at", null)
    .limit(50); // puedes ajustar

  if (error) {
    throw new Error(`Error leyendo post_feedback: ${error.message}`);
  }

  if (!pending || pending.length === 0) {
    console.log("[FEEDBACK_COLLECT] No hay posts pendientes");
    return;
  }

  console.log(
    `[FEEDBACK_COLLECT] Procesando ${pending.length} posts pendientes`
  );

  for (const row of pending) {
    try {
      const metrics = await fetchMetricsForMedia(row.ig_media_id);

      // 2) Actualizar post_feedback
      const { error: updateFeedbackError } = await supabase
        .from("post_feedback")
        .update({
          metrics,
          collected_at: new Date().toISOString(),
        })
        .eq("id", row.id);

      if (updateFeedbackError) {
        console.error(
          "Error actualizando post_feedback:",
          updateFeedbackError.message
        );
        continue;
      }

      // 3) Cargar generated_post → producto + estilo
      const { data: post, error: postError } = await supabase
        .from("generated_posts")
        .select("id, product_id, style, channel_published")
        .eq("id", row.generated_post_id)
        .single();

      if (postError || !post) {
        console.error(
          "No se pudo cargar generated_post para feedback:",
          postError?.message
        );
        continue;
      }

      const likes = Number(metrics.like_count || 0);
      const comments = Number(metrics.comments_count || 0);
      const impressions = Number(metrics.impressions || 0);
      const perf = computePerfScore(likes, comments);

      await updateProductPerformance(post.product_id, perf);
      await updateStylePerformance(
        post.style,
        post.channel_published || row.channel,
        impressions,
        perf
      );
    } catch (e: any) {
      console.error(
        `[FEEDBACK_COLLECT] Error procesando ${row.ig_media_id}:`,
        e.message
      );
    }
  }
}

async function fetchMetricsForMedia(igMediaId: string) {
  const url = `https://graph.facebook.com/${META_GRAPH_VERSION}/${igMediaId}?fields=like_count,comments_count,save_count,reach,impressions&access_token=${META_ACCESS_TOKEN}`;

  const res = await fetch(url);
  const json: any = await res.json();

  if (!res.ok) {
    throw new Error(
      `Error Graph API metrics (${res.status}): ${JSON.stringify(json)}`
    );
  }

  // devolvemos el JSON tal cual (filtrable luego)
  return json;
}

// simple EMA: 70% viejo, 30% nuevo
async function updateProductPerformance(
  productId: number,
  perf: number
): Promise<void> {
  const { data: existing } = await supabase
    .from("product_performance")
    .select("perf_score")
    .eq("product_id", productId)
    .maybeSingle();

  let newPerf = perf;
  if (existing && typeof existing.perf_score === "number") {
    newPerf = existing.perf_score * 0.7 + perf * 0.3;
  }

  const { error } = await supabase.from("product_performance").upsert(
    {
      product_id: productId,
      perf_score: newPerf,
      last_updated: new Date().toISOString(),
    },
    { onConflict: "product_id" }
  );

  if (error) {
    console.error("Error upsert product_performance:", error.message);
  } else {
    console.log(
      `[product_performance] product_id=${productId} perf=${newPerf.toFixed(2)}`
    );
  }
}

async function updateStylePerformance(
  style: string,
  channel: string,
  impressions: number,
  perf: number
): Promise<void> {
  const key = { style, channel };

  const { data: existing } = await supabase
    .from("style_performance")
    .select("impressions, perf_score")
    .eq("style", style)
    .eq("channel", channel)
    .maybeSingle();

  let totalImpressions = impressions;
  let totalPerf = perf;

  if (existing) {
    totalImpressions += Number(existing.impressions || 0);
    totalPerf += Number(existing.perf_score || 0);
  }

  const engagement =
    totalImpressions > 0 ? totalPerf / totalImpressions : 0;

  const { error } = await supabase.from("style_performance").upsert(
    {
      style,
      channel,
      impressions: totalImpressions,
      perf_score: totalPerf,
      engagement,
      last_updated: new Date().toISOString(),
    },
    { onConflict: "style,channel" }
  );

  if (error) {
    console.error("Error upsert style_performance:", error.message);
  } else {
    console.log(
      `[style_performance] style=${style} channel=${channel} impressions=${totalImpressions} perf_total=${totalPerf}`
    );
  }
}
