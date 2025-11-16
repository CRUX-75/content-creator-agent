// src/workers/feedbackLoop.pipeline.ts
// Pipeline FEEDBACK_LOOP (versión mínima):
// - Lee posts PUBLISHED recientes
// - Genera métricas dummy
// - Upsert en post_feedback
// - Actualiza product_performance y style_performance

import { supabase } from "../db/supabase.js";
import { logger } from "../utils/logger.js";

export type FeedbackLoopJob = {
  id: string;
  type: "FEEDBACK_LOOP";
  payload: {
    limit?: number;
  };
};

type Metrics = {
  like_count: number;
  comments_count: number;
};

type FeedbackResult = {
  post_id: string;
  product_id: number;
  style: string;
  channel: string;
  ig_media_id: string;
  metrics: Metrics;
};

export async function runFeedbackLoopPipeline(
  job: FeedbackLoopJob
): Promise<void> {
  logger.info({ jobId: job.id }, "[FEEDBACK_LOOP] start (minimal)");

  const limit = job.payload?.limit ?? 50;
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

  // 1) Posts PUBLISHED recientes con meta_post_id
  const { data: posts, error } = await supabase
    .from("generated_posts" as any)
    .select(
      "id, product_id, style, channel_published, published_at, meta_post_id"
    )
    .eq("status", "PUBLISHED")
    .not("meta_post_id", "is", null)
    .gte("published_at", since)
    .limit(limit);

  if (error) {
    logger.error(
      { jobId: job.id, error },
      "[FEEDBACK_LOOP] read PUBLISHED error"
    );
    throw error;
  }

  if (!posts || posts.length === 0) {
    logger.info({ jobId: job.id }, "[FEEDBACK_LOOP] nothing to collect");
    return;
  }

  logger.info(
    { jobId: job.id, count: posts.length },
    "[FEEDBACK_LOOP] Posts PUBLISHED encontrados"
  );

  const results: FeedbackResult[] = [];

  // 2) Generar métricas dummy por cada post
  for (const p of posts as any[]) {
    const mediaId = String(p.meta_post_id);

    // Métricas dummy: 1 like, 0 comentarios
    const metrics: Metrics = {
      like_count: 1,
      comments_count: 0,
    };

    results.push({
      post_id: p.id,
      product_id: p.product_id,
      style: p.style,
      channel: p.channel_published || "IG",
      ig_media_id: mediaId,
      metrics,
    });
  }

  if (results.length === 0) {
    logger.warn(
      { jobId: job.id },
      "[FEEDBACK_LOOP] No se han generado resultados de feedback"
    );
    return;
  }

  logger.info(
    { jobId: job.id, count: results.length },
    "[FEEDBACK_LOOP] Haciendo upsert en post_feedback"
  );

  // 3) Upsert en post_feedback (meta_post_id + ig_media_id)
  for (const r of results) {
    try {
      await supabase.from("post_feedback" as any).upsert(
        {
          generated_post_id: r.post_id,
          channel: r.channel,
          meta_post_id: r.ig_media_id,
          ig_media_id: r.ig_media_id,
          metrics: r.metrics as any,
          collected_at: new Date().toISOString(),
        } as any,
        { onConflict: "generated_post_id" } as any
      );
    } catch (e: any) {
      logger.warn(
        { jobId: job.id, post_id: r.post_id, err: e?.message },
        "[FEEDBACK_LOOP] upsert feedback failed"
      );
    }
  }

  logger.info(
    { jobId: job.id },
    "[FEEDBACK_LOOP] post_feedback actualizado (mínimo)"
  );

  // 4) Calcular perf_score y actualizar product/style performance
  // perf = likes + 2 * comments
  const prodMap = new Map<number, number>();
  const styleMap = new Map<string, number>();

  for (const r of results) {
    const perf =
      (r.metrics.like_count ?? 0) + 2 * (r.metrics.comments_count ?? 0);

    prodMap.set(r.product_id, (prodMap.get(r.product_id) ?? 0) + perf);

    const styleKey = `${r.style || "unknown"}|${r.channel}`;
    styleMap.set(styleKey, (styleMap.get(styleKey) ?? 0) + perf);
  }

  if (prodMap.size) {
    const productUpdates = Array.from(prodMap.entries()).map(
      ([product_id, perf_score]) => ({
        product_id,
        perf_score,
        last_updated: new Date().toISOString(),
      })
    );

    const { error: prodErr } = await supabase
      .from("product_performance" as any)
      .upsert(productUpdates as any, { onConflict: "product_id" } as any);

    if (prodErr) {
      logger.warn(
        { jobId: job.id, err: prodErr },
        "[FEEDBACK_LOOP] product_performance upsert failed"
      );
    }
  }

  if (styleMap.size) {
    const styleUpdates = Array.from(styleMap.entries()).map(
      ([key, perf_score]) => {
        const [style, channel] = key.split("|");
        return {
          style,
          channel,
          perf_score,
          last_updated: new Date().toISOString(),
        };
      }
    );

    const { error: styleErr } = await supabase
      .from("style_performance" as any)
      .upsert(styleUpdates as any, { onConflict: "style,channel" } as any);

    if (styleErr) {
      logger.warn(
        { jobId: job.id, err: styleErr },
        "[FEEDBACK_LOOP] style_performance upsert failed"
      );
    }
  }

  logger.info(
    { jobId: job.id, collected: results.length },
    "[FEEDBACK_LOOP] done (minimal)"
  );
}
