# coding=utf-8

# This file is the actual code for the Python runnable normalisation
import dataiku
from dataiku.runnables import Runnable

#import des fonctionnalités
from autodus import shared_dataset, utils
from autodus.process import normalisation_connexion_bigquery, normalisation_recette_sql, saving
from autodus.verif import verif_industrialisation, verif_recette_python
from autodus.output import macro_output
from autodus.logger.logger import Logger

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return (9,'None')
   

    def run(self, progress_callback): 
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        
        #définition du logger 
        macro_logger = Logger(raise_normalisation_connexion_bq_project_not_find=True,raise_table_should_be_industrialized=self.config['error_table_to_industrialize'])
        
        #dictionnaire des résultats des différentes actions
        results = dict()
        
        #connexion au projet
        client = dataiku.api_client()
        
        
        if self.project_key in client.list_project_keys():
            print("Clé projet valide")
            project = client.get_project(self.project_key)
        else:
            raise Exception("La clé de projet {:} n'existe pas.".format(self.project_key))
        
        progress_callback(1)
    
        #vérification de la sauvegarde
        results['save']= saving.executeSavingProcess(client=client,project=project,is_simulation=self.config['is_simulation'],logger=macro_logger)
        progress_callback(2)
        
        #vérification des appels de connexion dans les recettes Python
        results['connexion_python_recipe'] = verif_recette_python.isBigqueryCalledInsidePythonRecipe(project=project) if self.config['verif_python_recipe_connexion'] else None
        progress_callback(3)
        
        #récupération des shared dataset
        sharedDatasets = shared_dataset.get_shared_datasets(project=project,logger=macro_logger)
        sharedDatasets = shared_dataset.map_shared_datasets(client=client,projectkey=self.project_key,sharedDatasetss=sharedDatasets)
        results['shared_datasets'] = sharedDatasets
        progress_callback(4)
        
        # Remplissage du dictionnaire des variables d'environnement (instance et projet)
        env_variable_dict = utils.getDefaultEnvVariableDict(project_key=self.project_key)
        results["shouldBeIndustrialized"]=verif_industrialisation.shouldBeIndustrialized(env_variables=env_variable_dict,project=project,
                                                                                         shared_datasets=sharedDatasets,logger=macro_logger)
        progress_callback(5)
        
         # Création du dictionnaire des connexions
         #peut mettre jusqu'à 20 secondes d'exécution sur de grands projets
        dictOfConnexionWithName = utils.constructDictOfBigqueryConnexion(dict_env_variable=env_variable_dict,project=project,logger=macro_logger)    
        progress_callback(6)
        
        
        #Normalisation des recettes
        results["sql_recipe_normalization"]=normalisation_recette_sql.normaliseSQLRecipe(project=project,connexions_dict=dictOfConnexionWithName,
                                                                is_simulation=self.config['is_simulation'],env_variables=env_variable_dict,logger=macro_logger)
        progress_callback(7) 
        
        
        #Normalisation des connexions
        results["connexion_normalization"] = normalisation_connexion_bigquery.normaliseConnexions(project=project,datasets_to_industrialize=results["shouldBeIndustrialized"],
                                                                    is_simulation=self.config['is_simulation'],env_variables=env_variable_dict,logger=macro_logger)
        progress_callback(8)  
        
        macro_logger.raiseErrors()
        if not(self.config['is_simulation']):
            utils.save_last_normalisation_variable(project = project)
        progress_callback(9)  
        
        results['isSimulation'] = self.config['is_simulation']
            
        return macro_output.resultsToHtml(results,macro_logger)
