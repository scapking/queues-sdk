class QueuesError(RuntimeError):
    """queues-sdk 业务异常：对外 API 的显式失败（与内部守卫背压语义区分）。"""
