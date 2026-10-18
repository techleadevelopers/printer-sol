import { Router, type IRouter } from "express";
import type { HealthStatus } from "@workspace/api-zod";

const router: IRouter = Router();

router.get("/healthz", (_req, res) => {
  const data: HealthStatus = {
    status: "ok",
    eventMonitorStatus: "unknown",
    checkedAt: new Date(),
  };
  res.json(data);
});

export default router;
