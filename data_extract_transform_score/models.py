import math


class ModelsRegistry(object):
    """Registers a model name with a model class"""
    def __init__(self, model_name_class_tuples_list=None):

        hard_coded_model_name_class_tuples = [("Logistic regression", LogisticRegressionModel)]

        if model_name_class_tuples_list is None:
            model_name_class_tuples_list = hard_coded_model_name_class_tuples
        else:
            model_name_class_tuples_list += hard_coded_model_name_class_tuples

        self.model_name_class_dict = {}
        for model_name_class in model_name_class_tuples_list:
            model_name, model_class = model_name_class
            self.model_name_class_dict[model_name] = model_class


class PredictiveModel(object):

    def __init__(self, coefficients):
        self.coefficients = coefficients

    def score(self, input_dict):
        return (0.0, None)


class LogisticRegressionModel(PredictiveModel):

    def score(self, input_dict):

        input_dict["intercept"] = 1.0

        coefficients_included = []
        variables_included = []
        for key in input_dict:
            if key in self.coefficients:
                variables_included += [key]
                coefficients_included += [self.coefficients[key] * input_dict[key]]

        coefficients_included_paired = self._pair_with_coefficients(variables_included, coefficients_included)

        variables_not_included = []
        coefficients_not_included = []
        for key in self.coefficients:
            if key not in variables_included:
                variables_not_included += [key]
                coefficients_not_included += [self.coefficients[key]]

        coefficients_not_included_pairs = self._pair_with_coefficients(variables_not_included, coefficients_not_included)

        return (self._compute_score_using_logistic(coefficients_included),
                {"coefficients_included": coefficients_included_paired,
                 "coefficients_not_included": coefficients_not_included_pairs})

    def _pair_with_coefficients(self, pair1, pair2):

        paired_list = [(pair1[i], pair2[i]) for i in range(len(pair1))]
        paired_list.sort(key=lambda x: x[1], reverse=True)

        return paired_list

    def _compute_score_using_logistic(self, coefficients):
        return math.exp(sum(coefficients)) / (1 + math.exp(sum(coefficients)))


class MultipleKeyedModels(PredictiveModel):
    pass


class HTTPRestModel(PredictiveModel):
    pass


class OpenScoringRestModel(HTTPRestModel):
    pass
