import uuid

import numpy as np

from decs.backends import RedisBackend
from decs.schemas import DurerSchema


class SelectedImage(DurerSchema):
    image: np.ndarray[np.uint8]


class MaskedEdit(DurerSchema):
    mask: np.ndarray[np.uint8]
    prompt: str = ""


def main():
    ecs_backend = RedisBackend()
    uid = uuid.uuid4()

    # requester
    entity = ecs_backend.entity(uid)

    image = np.random.randint(0, 255, (512, 512, 3), dtype=np.uint8).astype(np.uint8)
    entity.set(SelectedImage, SelectedImage(image=image))

    mask = np.random.randint(0, 255, (512, 512, 1), dtype=np.uint8).astype(np.uint8)
    entity.set(MaskedEdit, MaskedEdit(mask=mask, prompt="a frog holding a torch"))

    # worker
    ecs_backend = RedisBackend()
    entity = ecs_backend.entity(uid)
    if not entity.has(SelectedImage) or not entity.has(MaskedEdit):
        raise ValueError(
            "Entity does not have all required components and we are "
            "somehow inside the worker body for this. This is a bug."
        )

    image = entity.get(SelectedImage)
    edit = entity.get(MaskedEdit)

    print(image.image.shape)
    print(image)
    print(edit.mask.shape)
    print(edit.prompt)

    # do the edit


if __name__ == "__main__":
    main()
