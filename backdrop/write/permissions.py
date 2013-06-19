class Permissions(object):
    def __init__(self, permissions):
        self._permissions = permissions

    def allowed(self, user, bucket_name):
        return self._has_permission(user, bucket_name) \
            or self._has_wildcard_permission(user)

    def _has_wildcard_permission(self, user):
        return self._has_permission(user, "*")

    def _has_permission(self, user, bucket_name):
        return bucket_name in self._permissions.get(user, [])
