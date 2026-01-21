
import omni
from omni.isaac.core.utils.prims import get_all_matching_child_prims
stage = omni.usd.get_context().get_stage()
for prim in stage.Traverse():
    if "uav" in str(prim.GetPath()):
        print(f"Found prim: {prim.GetPath()} type: {prim.GetTypeName()}")
