from autodus import utils
import re 
import sqlparse

#configuration 

#chaîne unique avec laquelle remplacer les "-" des noms de connexion d'une requête
#on doit réaliser cette action car le caractère "-" n'est pas autorisé de base dans le parseur des connexions SQL
unique_string_replacement = "_uANiQuEèèèèèsTriNgàAàAà_"

#chaîne unique pour délimiter les variables TBL des connexions
unique_pattern_tbl = ("èTèBèLèEèNèVè1è.2è3è4è5è6","é6é5é4é3é2é.1éTéBéLéEéNéVé")
#chaîne unique pour délimiter les variables d'environnement des connexions
unique_pattern_general = ("èEèNèVè1è2è3è4è5è6è","é6é5é4é3é2é1éEéNéVé")

#apply tbl normalization in connexion
def apply_tbl_normalization(query,inputs_table,connection_dict,env_variables,logger,name):
    new_tokens = []
    before_after_normalization = []
    
    token_list = list(sqlparse.parse(query)[0].flatten())
    token_list_updated = []
    #get comment part and query part of a SQL query
    i=0
    while i < len(token_list):
        if str(token_list[i].ttype) in ["Token.Comment.Single","Token.Comment.Multiline"]:
            token_list_updated.append(('comment',token_list[i].value))
            i+=1
        else:
            chaine = ""
            while i<len(token_list) and not str(token_list[i].ttype) in ["Token.Comment.Single","Token.Comment.Multiline"]:
                chaine += token_list[i].value
                i+=1
            token_list_updated.append(('query',chaine))
    
    #for each part of query
    for t in token_list_updated:
        #reconstruction of the query
        token_cleaned = convert_query_without_variable_with(t[1].replace(unique_string_replacement,"-"))
        
        #if its a comment part, dont update connexions
        if t[0]=='comment':
            new_tokens.append(token_cleaned)
        else:
            #identify connexions
            detected_tables = utils.catchSqlConnexionFromString(string=token_cleaned,env_variables=env_variables,logger=logger)
            detected_tables = [v[0].strip() for v in detected_tables]
            
            for conn in detected_tables:
                new_connexion = utils.addDefaultProjectToConnexion(env_variables,conn,logger=logger,typeValue="Recette",nameOfSource=None)
                #use tbl variable    
                if new_connexion in connection_dict:
                    candidate = [v for v in connection_dict[new_connexion] if v in inputs_table]
                    if len(candidate) == 0:
                        logger.add_log(severity="ERROR_TO_RAISE",topic="NORMALISATION_RECETTE_SQL",description="{:} (Recette : {:})".format(new_connexion,name))
                    elif len(candidate)>1:
                            raise Exception("Plus d'un dataset en entrée de la recette {:} utilise une même connexion : {:}.".format(name,new_connexion))
                    else:
                        new_conn=" ${tbl:"+candidate[0]+"} "
                        token_cleaned = token_cleaned.replace(conn,new_conn)
                        before_after_normalization.append((conn,new_conn))                                                    
                else:
                    logger.add_log(severity="ERROR_TO_RAISE",topic="NORMALISATION_RECETTE_SQL",description="{:} (Recette : {:})".format(new_connexion,name))
                    
            new_tokens.append(token_cleaned)
                        
    sql = ''.join([t for t in new_tokens])
    #format the updated sql recipe  
    return sqlparse.format(sql, reindent_aligned=True),before_after_normalization

#decode all variable that is coded
def convert_query_without_variable_with(query):  
    pattern_tbl = r'{:}([0-9A-Za-z\-|_|.|:]*?){:}'.format(unique_pattern_tbl[0],unique_pattern_tbl[1])
    pattern_pattern_general = r'{:}([0-9A-Za-z\-|_|.|:]*?){:}'.format(unique_pattern_general[0],unique_pattern_general[1])

    result_env_var_dataset_name = re.findall(pattern_tbl, query)
    for v in result_env_var_dataset_name:
        query = query.replace(unique_pattern_tbl[0]+v+unique_pattern_tbl[1],"${tbl:"+v+"}")

    result_env_var_dataset_name = re.findall(pattern_pattern_general, query)
    for v in result_env_var_dataset_name:
        query = query.replace(unique_pattern_general[0]+v+unique_pattern_general[1],"${"+v+"}")

    return query

#code all environment variable 
def convert_query_without_variable(query):
    pattern = r'\$\{(([0-9A-Za-z]+[-|_|.|:]*)+)\}'
    result_env_var_dataset_name = re.findall(pattern, query)
    for v in result_env_var_dataset_name:
        v= v[0]
        #convert_tbl_to_text
        if v[:4].lower() == "tbl:":
             query = query.replace("${"+v+"}",unique_pattern_tbl[0]+v[4:]+unique_pattern_tbl[1])
        #convert basic variable to text
        else:
             query = query.replace("${"+v+"}",unique_pattern_general[0]+v+unique_pattern_general[1])
    return query

#method that clean query 
#encode variable to be correctly interpreted by the parser
#encode dash to be correctly interpreted by the parser 
def clean_query(query):
    
    #convert environment variable
    query = convert_query_without_variable(query)

    parser = sqlparse.parse(query)
    token_list = list(parser[0].flatten())
    
    #dash detection in name 
    find= False
    chaine_list = []
    chaine = ""
    i=1
    while i<len(token_list)-1:
        if i== len(token_list)-1:
            if find:
                chaine_list.append(chaine)
                break
        if str(token_list[i-1].ttype) == "Token.Name" and str(token_list[i+1].ttype) == "Token.Name" and token_list[i].value=="-":
            if not find:
                find=True
                chaine+=token_list[i-1].value
            chaine+="-"+token_list[i+1].value
            i = i+2
        else:
            if find:
                chaine_list.append(chaine)
                find = False
                chaine = ""

            i+=1
    
    #replace dash in connexion name with unique string
    for stri in chaine_list:
        query = query.replace(stri,stri.replace("-",unique_string_replacement))

    return query

# Essaye de remplacer les connexions des recettes SQL par la variable générique TBL
def normaliseWithTBL(text=None,setting=None,connection_dict=None,name=None,project=None,env_variables=None,logger=None):

    inputsTable = [project.get_dataset(i).name for i in setting.get_flat_input_refs()]
                         
    clean_query = clean_query(text,inputsTable,connection_dict,env_variables,logger,name)

    return apply_tbl_normalization(clean_query,inputsTable,connection_dict,env_variables,logger,name)