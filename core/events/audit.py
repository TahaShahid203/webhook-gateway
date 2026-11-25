from .models import AuditLog


def audit(actor: str, action: str, object_type: str = "", object_id="", old="", new="", **detail) -> AuditLog:
    return AuditLog.objects.create(actor=actor, action=action, object_type=object_type,
                                   object_id=str(object_id), old_value=str(old), new_value=str(new), detail=detail)
