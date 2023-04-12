import pandas as pd
from autodus import utils
import copy

#normalisation connexions -----------------------------------------------------------------------------------------------
def normaliseConnexions(project=None,datasets_to_industrialize=None,is_simulation=None,env_variables=None,logger=None):
    projectKey = project.get_summary()['projectKey']
    normaliseConnexionsObject = {"nb_connexion":0,"nb_connexion_normalise":0,"dataframe_html":None}
    dataset_column_name = {"dku_dataset" :"Nom du dataset","catalog_original":"Nom du projet (Avant)","schema_original":"Nom du dataset (Avant)", "table_original":"Nom de la table (Avant)",
                          "catalog_updated":"Nom du projet (Après)","schema_updated":"Nom du dataset (Après)", "table_updated":"Nom de la table (Après)"}
    
    datasetControlColName = [dataset_column_name['dku_dataset'], dataset_column_name['catalog_original'],dataset_column_name['schema_original'], dataset_column_name['table_original'],
                             dataset_column_name['catalog_updated'],dataset_column_name['schema_updated'], dataset_column_name['table_updated']]
    datasetControlDf = pd.DataFrame(columns=datasetControlColName)

    for datasetItem in project.list_datasets():
        if datasetItem.type=="BigQuery":
            dataset = datasetItem.to_dataset()
            settings = dataset.get_settings()
            sqlParams  = settings.get_raw_params()

            #Initialisation des parametres à mettre à jour
            updatedsqlParams = copy.deepcopy(sqlParams)
            #Variabilisation des schémas
            updatedsqlParams['schema'] = updatedsqlParams['schema']\
            .replace('DWZGRP01', 'DWZGR${env_sid}')\
            .replace('prod_', '${env_edh}_')\
            .replace('_lab_', '_${env_type}_')      

            #Si projet non renseigné, on impose une valeur par défaut
            if not ('catalog' in sqlParams) or len(updatedsqlParams['catalog'])==0:
                updatedsqlParams['catalog'] = '${RETARGET_ME}'

            #MaJ projet : bascule simple sur les variables    
            updatedsqlParams['catalog'] = updatedsqlParams['catalog']\
            .replace('pit-edh0labue-p-arrm', '${RETARGET_ME}')\
            .replace('pit-edh0ing-p-ph59', '${bq_project_edhing}')\
            .replace('pit-edh0pro-p-u6vm', '${bq_project_edhpro}')\
            .replace('pit-sid0side-p-k4ut', '${bq_project_side}')\
            .replace('pit-sid0sido-p-dwx0', '${bq_project_sido}')\
            .replace('pit-sid0sidr-p-whjh', '${bq_project_sidr}')

            #Dans le cas où l'on est sur le projet lab, reciblage ou suppression de l'information selon nom table 
            if (updatedsqlParams['catalog']=='${RETARGET_ME}'):
                if updatedsqlParams['schema'].startswith('DWZGR') and updatedsqlParams['table'].startswith('DE_'):
                    updatedsqlParams['catalog']='${bq_project_side}'
                elif updatedsqlParams['schema'].startswith('DWZGR') and updatedsqlParams['table'].startswith('DO_'):
                    updatedsqlParams['catalog']='${bq_project_sido}'
                elif updatedsqlParams['schema'].startswith('DWZGR') and updatedsqlParams['table'].startswith('DR_'):
                    updatedsqlParams['catalog']='${bq_project_sidr}'
                elif updatedsqlParams['schema'].startswith('${env_edh}_v_dlk'):
                    updatedsqlParams['catalog']='${bq_project_edhing}'
                elif updatedsqlParams['schema'].startswith('${env_edh}_v_hub') or updatedsqlParams['schema'].startswith('${env_edh}_v_spc'):
                    updatedsqlParams['catalog']='${bq_project_edhpro}'
                elif (updatedsqlParams['table'].startswith('${projectKey}_')) or (updatedsqlParams['schema'].startswith('${env_edh}_${env_type}_')):
                    updatedsqlParams['catalog']='${bq_current_project}'

            #Ne normalise pas les tables qui ne sont pas industrialisées
            project_name = updatedsqlParams['catalog']+"." if "catalog" in updatedsqlParams else ""
            if utils.replaceEnvVariableByValue(default_dict_env_variable=env_variables,txt=project_name+updatedsqlParams['schema']+"."+updatedsqlParams['table'],
                                               typeValue="Dataset",nameOfSource=datasetItem.name,logger=logger) in datasets_to_industrialize:
                updatedsqlParams['catalog']='${bq_project_labue}'
                updatedsqlParams['schema'] = updatedsqlParams['schema'].replace('_${env_type}_','_lab_') 

            #Si un projet de connexion BigQuery n'a pas été identifié
            if updatedsqlParams['catalog']=='${RETARGET_ME}':
                logger.add_log(severity="ERROR_TO_RAISE",topic="NORMALISATION_CONNEXION_BQ_PROJECT_NOT_FIND",description=updatedsqlParams['schema']+"."+updatedsqlParams['table'] + " ({:})".format(dataset.name))

            catalogueOri = sqlParams['catalog'] if 'catalog' in sqlParams else ''
            diff = 'Yes' if sqlParams['schema']!=updatedsqlParams['schema'] or sqlParams['table'] != updatedsqlParams['table'] or catalogueOri != updatedsqlParams['catalog'] else 'No'
            datasetControlDf=datasetControlDf.append({dataset_column_name['dku_dataset'] : dataset.name, dataset_column_name['catalog_original'] : catalogueOri,
                                                      dataset_column_name['schema_original'] : sqlParams['schema'], dataset_column_name['table_original'] : sqlParams['table'],
                                                      dataset_column_name['catalog_updated'] : updatedsqlParams['catalog'],dataset_column_name['schema_updated'] : updatedsqlParams['schema'],
                                                       dataset_column_name['table_updated'] : updatedsqlParams['table'], 'Difference':diff}, ignore_index=True)

            #Si référence en dur à la clé projet dans la table, bascule vers la variable
            updatedsqlParams['table']=updatedsqlParams['table'].replace(projectKey,'${projectKey}')

            #Si pas de simulation, enregister
            if not is_simulation:
                logger.raiseErrors()
                sqlParams['catalog']=updatedsqlParams['catalog']
                sqlParams['schema'] = updatedsqlParams['schema']
                sqlParams['table'] = updatedsqlParams['table']
                settings.save()

    #Afficher le bilan AVANT/APRES si des recettes BigQuery sont présentes dans le flow du projet
    if len(datasetControlDf)>0:
        normaliseConnexionsObject['nb_connexion']=len(datasetControlDf)
        datasetControlDf = datasetControlDf[datasetControlDf['Difference']=='Yes'].drop(['Difference'], axis=1)
        normaliseConnexionsObject['nb_connexion_normalise']=len(datasetControlDf)
        #Si des différences sont présentes
        if len(datasetControlDf)>0:
            normaliseConnexionsObject['dataframe_html']=datasetControlDf.to_html(escape=False,index=False)
        
    return normaliseConnexionsObject

