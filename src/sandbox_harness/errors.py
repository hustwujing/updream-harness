"""Stable public failure reasons, independent of provider exception classes."""
import re


def public_failure(code: str, detail: str) -> tuple[str, str]:
    text = detail.lower()
    if re.search(r'\b503\b', text) or 'servers are currently overloaded' in text:
        return 'model_service_overloaded', '模型服务繁忙（503），本轮未能完成。已保存的阶段文件可在工作区查看，稍后可以继续。'
    if re.search(r'\b429\b', text):
        return 'model_rate_limited', '模型服务限流（429），请稍后继续。'
    messages = {
        'RuntimeStreamInterrupted': '与沙箱的连接中断，自动重连未能取回执行结果。本轮已停止；部分操作可能已执行，请核对工作区后再继续。',
        'HTTPStatusError': '沙箱服务返回异常，本轮未能完成。请稍后继续。',
        'CheckpointRestoreError': '会话工作区恢复失败，请稍后重试。',
        'ConnectError': '暂时无法连接沙箱，请稍后重试。',
        'harness_result_missing': '执行进程已退出，但没有确认完成。本轮未计为成功，请检查已保存的文件后继续。',
        'harness_protocol_error': '执行进程返回了无法处理的消息，本轮已停止，请检查已保存的文件后重试。',
        'input_delivery_failed': '消息未能确认送达执行进程，本轮已停止，请重新发送。',
        'harness_exit': '执行进程异常退出，请检查已保存的文件后继续。',
        'model_response_timeout': '模型服务在规定时间内没有回复，请稍后重试或切换模型。',
        'harness_idle_timeout': '执行长时间没有新进展，本轮已停止。',
        'cleanup_failed': '运行结果保存或环境清理失败，请稍后重试。',
    }
    if code == 'RuntimeError' and 'stream ended without a result' in text:
        code = 'RuntimeStreamInterrupted'
    return code, messages.get(code, '本轮执行遇到异常，请检查已保存的文件后再继续。')
