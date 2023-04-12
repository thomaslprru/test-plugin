from dataikuapi.utils import DataikuException
from autodus.exception.exceptions import ProjectAccessRightException

# Permet de récupérer les shared dataset d'un projet
def get_shared_datasets(project=None,logger=None):
    managedFolder=[mf['id'] for mf in project.list_managed_folders()]
    datasets = set()
    for recipeItem in project.list_recipes():
        recipe = project.get_recipe(recipeItem.name)
        settings = recipe.get_settings()
        if "main" in settings.get_recipe_inputs():
            for ds in settings.get_recipe_inputs()['main']['items']:
                #on vérifie que le dataset n'est pas un managedfolder
                if ds['ref'] not in managedFolder:
                    datasets.add(ds['ref'])
        elif "source_bq" in settings.get_recipe_inputs():
            for ds in settings.get_recipe_inputs()['source_bq']['items']:
                if ds['ref'] not in managedFolder:
                    datasets.add(ds['ref'])
        else:
            if not(settings.get_recipe_inputs()=={}):
                logger.add_log(severity="WARNING",topic="SHARED DATASET",description="Il se peut qu'un shared dataset n'est pas été détecté. Veuillez contacter l'adminisatreur du script pour plus de précision.")


    sharedDatasetss = set()
    for ds in datasets:
        try:
            project.get_dataset(ds).get_settings().get_raw()
        except DataikuException as err:
            if "dataset does not exist" in err.args[0]:
                sharedDatasetss.add(ds)
    return sharedDatasetss


# Permet de récupérer les informations d'un shared dataset dans son projet parent
# Il faut bénéficier des droits de lecture sur le projet parent sinon la méthode lève une exception
def map_shared_datasets(client=None,projectkey=None,sharedDatasetss=None):
    datasets = []
    for project_key in sharedDatasetss:
        project_key = project_key.split(".")[0]
        try:
            p = client.get_project(project_key)
            for exposed_object in p.get_settings().get_raw()["exposedObjects"]["objects"]:
                if exposed_object['type']=="DATASET":
                    for rule in exposed_object["rules"]:
                        if rule["targetProject"]==projectkey and rule["appearOnFlow"]:
                            general_info = p.get_dataset(exposed_object["localName"]).get_info().get_raw()
                            connection = general_info['dataset']['params']
                            proj_variables = p.get_variables()['standard']
                            proj_variables['projectKey'] = general_info['dataset']['projectKey']
                            datasets.append({'type':general_info['type'],'name':general_info['name'],
                                            'projectKey':general_info['dataset']['projectKey'],'catalog':connection['catalog'],
                                             'connection':connection['connection'],'table':connection['table'],
                                             'schema':connection['schema'],'project_variables':proj_variables})
        except :
            raise ProjectAccessRightException("[Détection des shared datasets] Pas les droits de lecture sur le projet {:}.".format(project_key))

    return datasets