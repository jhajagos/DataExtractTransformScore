

class TransformationsRegistry():
    def __init__(self, local_transformations_tuples=None):

        hard_coded_transformation_tuples = [("Identity", lambda x: (x, None))]

        if local_transformations_tuples is None:
            transformation_names_tuples = local_transformations_tuples
        else:
            transformation_names_tuples = local_transformations_tuples + hard_coded_transformation_tuples

        self.transformation_name_dict = {}
        for transformation_name_tuple in transformation_names_tuples:
            transformation_name, transformation_func = transformation_name_tuple
            self.model_name_class_dict[transformation_name] = transformation_func
