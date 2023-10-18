from decs.backends import RedisBackend
from decs.entity import Entity
from decs.schemas import DecsSchema


def test_entity_works():
    class Position(DecsSchema):
        x: float = 0.0
        y: float = 0.0
        z: float = 0.0

    class Velocity(DecsSchema):
        x: float = 0.0
        y: float = 0.0
        z: float = 0.0

    class Names(DecsSchema):
        name: str
        alias: str | None = None

    for backend_class in (RedisBackend,):
        ecs_backend = backend_class()
        ecs_backend.clear()

        entity = ecs_backend.entity()
        assert isinstance(entity, Entity)

        start_position = Position(x=1.0, y=2.0, z=3.0)
        entity.set(start_position)
        position = entity.get(Position)
        assert isinstance(position, Position)
        assert position == start_position

        start_velocity = Velocity(x=3.0, y=2.0, z=1.0)
        entity.set(start_velocity)
        velocity = entity.get(Velocity)
        assert isinstance(velocity, Velocity)
        assert velocity == start_velocity

        timedelta = 0.1
        new_position = Position(
            x=position.x + timedelta * velocity.x,
            y=position.y + timedelta * velocity.y,
            z=position.z + timedelta * velocity.z,
        )

        entity.set(new_position)
        position = entity.get(Position)
        assert isinstance(position, Position)
        assert position == new_position

        assert not entity.has(Names)
        entity.set(Names(name="Camelia Fortress", alias="Knapsack"))
        assert entity.has(Names)
        names = entity.get(Names)
        assert isinstance(names, Names)
        assert names.name == "Camelia Fortress"
        assert names.alias == "Knapsack"
