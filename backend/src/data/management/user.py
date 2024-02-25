""" User business object """

from persistance.business_object_base import BO_Base
from persistance.bo_descriptors import BO_str

from core.app_logging import getLogger

LOG = getLogger(__name__)


class User(BO_Base):
    name = BO_str()
    role = BO_str()

    def __init__(self, id=None, name: str = None) -> None:
        super().__init__(id=id)
        self.name = name

    def __repr__(self) -> str:
        return f"<User id:{self.id}, name:{self.name}>"


# LOG.debug(f"{BO_Base._business_objects=}")
