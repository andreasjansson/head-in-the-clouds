# TODO:
#
# * warn if trying to close port 22
#
# * restart_strategy: {restart_before (default), restart_after}
#   - this requires that you can rename containers, which you can't at
#     the moment
#
# * refactor and document and make nice
#   - especially "stupid_json_hack" and stuff around bid
#
# * support explicit $depends clause?
#   - might be a use case with containers waiting for other containers 
#     to start before they can


from headintheclouds.ensemble.tasks import up
__all__ = ['up']
