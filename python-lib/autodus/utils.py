import dataiku
import re
from datetime import datetime

#typeValue in [recette, dataset]
#déréférence toutes les variables sauf celle content ${tbl:......}
def replaceEnvVariableByValue(default_dict_env_variable=None,txt=None,typeValue=None,nameOfSource=None,specificEnvVariable=None,logger=None):
    #copying default env variable dict
    variables_env = default_dict_env_variable
    # Remplacement des variables d'environnement
    pattern = r'\$\{(([0-9A-Za-z]+[-|_|.|:]*)+)\}'
    result_env_var_dataset_name = re.findall(pattern, txt)
    result_env_var_dataset_name = [v[0] for v in result_env_var_dataset_name]
    new_txt = txt

    if specificEnvVariable is not None:
        for k,v in specificEnvVariable.items():
            variables_env[k]=v  
    for item in result_env_var_dataset_name:
        if item in variables_env:
            new_txt = new_txt.replace('${'+item+"}",variables_env.get(item))
        elif not(item[:4]=="tbl:"):
            logger.add_log(severity="ERROR_TO_RAISE",topic="DENORMALISATION",description="{:} ({:}) | {:} - {:}".format(item,txt,typeValue,nameOfSource))
       
    new_txt = new_txt.replace("`","")
    if len(new_txt) and "." in new_txt[0]+new_txt[-1] or ".." in new_txt:
        raise Exception("Problème de déréférencement : la connexion {:} est devenue {:}. Il se peut qu'une variable de projet soit vide.".format(txt,new_txt))
        
    return new_txt

# Retourne les variables d'environnement d'un projet
def getDefaultEnvVariableDict(project_key=None):
    variables_env = {}
    for key, value in dataiku.get_custom_variables().items():
        variables_env[key]=value

    variables_env['projectKey']=project_key
    return variables_env

#pour un projet, cette méthode construit un dictionnaire pour chaque dataset avec
#key = connexion du dataset --> value = nom(s) du dataset
def constructDictOfBigqueryConnexion(dict_env_variable=None,project=None,logger=None):
    connexions = {}
    for ds in project.list_datasets(as_type="objects"):
        raw_params = ds.get_settings().get_raw_params()
        url = raw_params.get('catalog')+"." if raw_params.get('catalog') and not(raw_params.get('catalog')=="")  else ""
        url += raw_params.get('schema')+"." if raw_params.get('schema') and not(raw_params.get('schema')=="") else ""
        url += raw_params.get('table') if raw_params.get('table') else ""
        url = replaceEnvVariableByValue(default_dict_env_variable=dict_env_variable,txt=url,typeValue="Dataset",nameOfSource=ds.name,logger=logger)
        if len(url)>0:
            connexion_updated = addDefaultProjectToConnexion(dict_env_variable,url,logger=logger,typeValue="Dataset",nameOfSource=ds.name)
            if connexion_updated not in connexions:
                connexions[connexion_updated] = []
            connexions[connexion_updated].append(ds.name)
            
    return connexions

#si on a une connexion de type NOMDUDATASET.NOMDETABLE 
#alors la fonction rajoute le default project
def addDefaultProjectToConnexion(dictEnvVariable,connexion,typeValue=None,nameOfSource=None,logger=None):
    
    if len(connexion.split("."))<3:
        connexion = "${bq_current_project}."+connexion.replace(" ","")

    return replaceEnvVariableByValue(default_dict_env_variable=dictEnvVariable,txt=connexion,typeValue=typeValue,
                                     nameOfSource=nameOfSource,logger=logger)

#Interception des connexions
# définir une liste de dataset de départ valide
def catchSqlConnexionFromString(string=None,env_variables=None,logger=None):
    validDatasetStart = ["DWZGR","prod_"]
    pattern =r'([ `][0-9A-Za-z\$\_]{1,}[0-9A-Za-z{}\-\$\_]{4,}\.[0-9A-Za-z\$\_]{1,}[0-9A-Za-z{}\-\$\_]{4,}(\.[0-9A-Za-z\$\_]{1,}[0-9A-Za-z{}\-\$\_]{3,})?[ .\'"`])'
    ori =[v[0] for v in re.findall(pattern, string)]
    res = []
    #on insert la connexion s'il s'agit d'une connexion avec un dataset valide
    #permet d'alléger la regex
    for v in range(len(ori)):
        canInsert = False
        connexion_with_default_project = addDefaultProjectToConnexion(env_variables,ori[v],logger=logger,typeValue="Recette",nameOfSource=None)
        for pre in validDatasetStart:
            if connexion_with_default_project.split(".")[1].startswith(pre):
                canInsert=True
                break
        if canInsert:
            res.append((ori[v],connexion_with_default_project))
        
    return res


# Remplacer la clé de projet saisie de manière brute dans une connexion en variable générique (${project_key})
def normalizeProjectKeyInConnexion(sql_query=None,project_key=None,env_variables=None,logger=None):
    res_regex = catchSqlConnexionFromString(string=sql_query,env_variables=env_variables,logger=logger)
    res_regex_replaced = [v[1].replace(project_key,"${projectKey}") for v in res_regex]
    for v in range(len(res_regex)):
        sql_query = sql_query.replace(res_regex[v][1],res_regex_replaced[v])
    return sql_query

# Ajoute une variable de projet contenant la date de la dernière normalisation appliquée via la macro
def save_last_normalisation_variable(project = None):
    projectVariables = project.get_variables()
    # current date and time
    now = datetime.now() 
    #create or update last_normalisation_with_macro project variable 2023-03-11T23:15:47Z
    projectVariables['standard']['IRIS-Industrialisation_last_normalisation'] = now.strftime("%Y-%m-%dT%H:%M:%SZ") 
    project.set_variables(projectVariables)
