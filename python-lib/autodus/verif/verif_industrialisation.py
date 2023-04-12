from autodus import utils 

# Cherche un dataset d'un projet par son nom
def findDatasetByName(dataset_list=None,name=None):
    for d in dataset_list:
        if "name" in d:
            if d['name']==name:
                return d
    return None

#si avant variabilisation on a des lab_ue dans des tables sources alors retourner un warning
def shouldBeIndustrialized(env_variables=None,project=None,shared_datasets=None,logger=None):
    recommendedReplication = []
    for ds in project.get_flow().get_graph().get_source_datasets():
        is_find = False
        try:
            ds_info_raw = ds.get_info().get_raw()
            if ds_info_raw['type'] == 'BigQuery':
                table = ds_info_raw['dataset']['params']['table']
                dataset_name = ds_info_raw['dataset']['params']['schema'].replace('_${env_type}_','_lab_')
                project_name = ds_info_raw['dataset']['params']['catalog']+"." if 'catalog' in ds_info_raw['dataset']['params'] else ""
                conn = utils.replaceEnvVariableByValue(default_dict_env_variable=env_variables,txt=project_name+dataset_name+"."+table,typeValue="Dataset",
                                                       nameOfSource=ds.name,logger=logger)
                is_find = True
        except:
            dataset_info = findDatasetByName(dataset_list=shared_datasets,name=ds.name.split(".")[-1])
            if dataset_info is not None:
                table = dataset_info['table']
                dataset_name = dataset_info['schema'].replace('_${env_type}_','_lab_')
                project_name = dataset_info['catalog']+"." if 'catalog' in dataset_info else ""
                conn = utils.replaceEnvVariableByValue(default_dict_env_variable=env_variables,txt=dataset_name+"."+table,typeValue="Dataset",nameOfSource=ds.name,
                                                       specificEnvVariable=dataset_info['project_variables'],logger=logger)
                is_find = True
        
        #!! attention : peut poser problème si un dataset contient dans le nom "_lab_" par exemple : ${env_edh}_v_dlk_testdataset_lab_ 
        projectKey = project.get_summary()['projectKey']
        if is_find and "_lab_" in dataset_name:
            if not("${projectKey}" in table or table[:len(projectKey)] == projectKey):
                recommendedReplication.append(conn)
                logger.add_log(severity="ERROR_TO_RAISE",topic="TABLE_SHOULD_BE_INDUSTRIALIZED",description=conn+" ({:})".format(dataset_name))
    
    return recommendedReplication