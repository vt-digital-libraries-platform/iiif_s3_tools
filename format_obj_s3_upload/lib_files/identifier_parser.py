import json

class IdentifierParser(object):
    def __init__(self, identifier, convention, conventions_file_path):
        self.identifier = identifier
        self.convention = convention
        self.conventions = None
        self.conventions_file_path = conventions_file_path
        try:
            with open(self.conventions_file_path) as f:
                self.conventions = json.load(f)
        except Exception as e:
            print(f"Error loading conventions file: {e}")

    def parse(self):
        ret_dict = {
            'identifier': None,
            'collection': None,
            'type': None,
            'first_enum': None,
            'second_enum': None
        }
        
        try:
          ret_dict['identifier'] = self.identifier[self.conventions[self.convention]['identifier_start']:self.conventions[self.convention]['identifier_end']]
        except Exception as e:
            print(f"Error parsing identifier: {e}")
        try:
          ret_dict['collection'] = self.identifier[self.conventions[self.convention]['collection_start']:self.conventions[self.convention]['collection_end']]
        except Exception as e:
            print(f"Error parsing identifier: {e}")
        try:
          ret_dict['type'] = self.identifier[self.conventions[self.convention]['type_start']:self.conventions[self.convention]['type_end']]
        except Exception as e:
            print(f"Error parsing identifier: {e}")
        try:
          ret_dict['first_enum'] = self.identifier[self.conventions[self.convention]['first_enum_start']:self.conventions[self.convention]['first_enum_end']]
        except Exception as e:
            print(f"Error parsing identifier: {e}")
        try:
          ret_dict['second_enum'] = self.identifier[self.conventions[self.convention]['second_enum_start']:self.conventions[self.convention]['second_enum_end']]
        except Exception as e:
            print(f"Error parsing identifier: {e}")
        return ret_dict
    
def get_item_data_by_convention(convention, identifier):
    identifier_parser = IdentifierParser(identifier, convention, "./lib_files/conventions.json")
    return identifier_parser.parse()