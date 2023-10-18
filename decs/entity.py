import uuid

from decs.abc import (
    BaseBackend,
    BaseEntity,
    Component,
    ComponentTypeDescriptor,
)


class Entity(BaseEntity):
    @staticmethod
    def from_uid(uid: uuid.UUID, backend: "BaseBackend"):
        return Entity(backend, uid)

    def has(self, component_type: ComponentTypeDescriptor) -> bool:
        return self._backend.get_component_storage(component_type).has(self)

    def uid(self):
        return self._uid

    def version(self):
        return 0

    def get(self, component_type: ComponentTypeDescriptor) -> Component | None:
        return self._backend.get_component_storage(component_type).get(self)

    def set(
        self,
        value: Component,
        component_type: ComponentTypeDescriptor | None = None,
        **kwargs,
    ):
        component_type = component_type or value.__class__
        storage = self._backend.get_component_storage(component_type)
        component_type = storage.component_type()

        # try in the following order: from_orm, dict, kwargs, direct
        coerced_successfully = isinstance(value, component_type)
        if not coerced_successfully:
            try:
                value = component_type.model_validate(value)
                coerced_successfully = True
            except TypeError:
                raise

        if not coerced_successfully:
            try:
                value = component_type(**value.model_dump())
            except TypeError:
                raise

        if not coerced_successfully:
            try:
                value = component_type(**value)
            except TypeError:
                raise

        if not coerced_successfully:
            value = component_type(value)

        storage.set(self, value, **kwargs)

    def remove(self, component_type: ComponentTypeDescriptor):
        self._backend.get_component_storage(component_type).remove(self)

    def __eq__(self, other: "BaseEntity"):
        return self._uid == other._uid
