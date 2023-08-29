

class RoutingData:
    # constructor
    def __init__(self,mdm_dict):
        self._document = mdm_dict


    # methods
    def _get_routing_data(self):
        '''
        Returns the routing data from the web part of mdd

        '''
        try:
            routing_data = self._document['xml']['mdm:metadata']['design']['routings']['scripts']['scripttype']['script']['#text']

            return routing_data
        except Exception as e:
            
            return "Something went wrong"
