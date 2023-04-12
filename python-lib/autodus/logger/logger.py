import pandas as pd
from autodus.exception.exceptions import NormalisationConnexionSQLException, TableAIndustrialiserException,DereferencementVariableException, NormalisationRecetteSqlException
pd.set_option('display.max_colwidth', -1)

#add style to dataframe (set line color to red for error and line color to orange for warning)
def highlight(row):
        if row['Sévérité'] == "ERROR":
            return ['background-color: red ; color: white ; font-weight: bold'] * len(row)
        elif row['Sévérité'] == "WARNING":
            return ['background-color: orange'] * len(row)
        else:
            return [''] * len(row)

#object to store all the event during de normalization process (info, warning, error)
class Logger:
    dataframe = None
    
    #constructor
    def __init__(self,raise_normalisation_connexion_bq_project_not_find = False, raise_table_should_be_industrialized=False):
        self.logs = []
        self.raise_normalisation_connexion_bq_project_not_find = raise_normalisation_connexion_bq_project_not_find
        self.raise_table_should_be_industrialized = raise_table_should_be_industrialized
        self.set_dataframe()

    #method to add new log to the logger    
    def add_log(self,severity=None,topic=None,description=None):
        if severity is None or description is None:
            raise Exception("Vous devez spécifier une sévérité du log et une description.\n  add_log(severity=?,description=?)")
        if severity not in ["INFO","WARNING","ERROR","ERROR_TO_RAISE"]:
            raise Exception("Le paramètre de sévérité peut seulement contenir les valeurs suivantes : INFO, WARNING, ERROR, ERROR_TO_RAISE")
        
        self.logs.append((severity,topic,description))
        self.set_dataframe()

    #method that check if there is error to raise
    #if yes, this method raise the exception corresponding to the specific error
    def raiseErrors(self):
        
        logs = self.dataframe[self.dataframe['Sévérité']=='ERROR_TO_RAISE'].groupby("Sujet")
        
        normalisation_connexion_bq_project_not_find_list = []
        table_should_be_industrialized_list = []
        denormalisation_list = []
        normalisation_recette_sql_list = []
        
        for name, group in logs:            
            for _, row in group.iterrows():
                if name =="NORMALISATION_CONNEXION_BQ_PROJECT_NOT_FIND":
                    normalisation_connexion_bq_project_not_find_list.append(row['Description'])
                elif name=="TABLE_SHOULD_BE_INDUSTRIALIZED":
                    table_should_be_industrialized_list.append(row['Description'])
                elif name=="DENORMALISATION":
                    denormalisation_list.append(row['Description'])
                elif name=="NORMALISATION_RECETTE_SQL":
                    normalisation_recette_sql_list.append(row['Description'])
                            
            if len(normalisation_recette_sql_list)>0:
                txtException = "Impossible de variabiliser avec TBL une ou plusieurs connexions : \n"
                for c in normalisation_recette_sql_list:
                    txtException += "  - {:} \n".format(c) 
                txtException += "Une ou plusieurs connexions sont écrites dans des recettes SQL mais ne font référence à aucun dataset du flow. \
                Il se peut que la connexion soit présente dans un commentaire (dans ce cas ignoré l'erreur) \n Il s'agit peut-être d'une erreur dans la spécification de la connexion."
                
                raise NormalisationRecetteSqlException(txtException,"NormalisationRecetteSqlException")
                    
            if len(denormalisation_list)>0:
                txtException = "Variables d'environnement inconnus : \n"
                for c in denormalisation_list:
                    txtException += "  - {:} \n".format(c) 
                txtException += "Une ou plusieurs variables sont utilisées dans le projet mais n'existent pas."
                raise DereferencementVariableException(txtException,"DereferencementVariableException")
            
            if self.raise_normalisation_connexion_bq_project_not_find and len(normalisation_connexion_bq_project_not_find_list)>0:
                txtException = "Impossible de trouver les noms de projet dans les connexions des datasets BigQuery suivants : \n"
                for c in normalisation_connexion_bq_project_not_find_list:
                    txtException += "  - {:} \n".format(c) 
                raise NormalisationConnexionSQLException(txtException,"NormalisationConnexionSQLException")
            
            if self.raise_table_should_be_industrialized and len(table_should_be_industrialized_list)>0:
                txtException = "Un ou plusieurs datasets doivent être industrialisés : \n"
                for c in table_should_be_industrialized_list:
                    txtException += "  - {:} \n".format(c) 
                raise TableAIndustrialiserException(txtException,"TableAIndustrialiserException")
    
    def get_nb_log(self):
        return {'all':len([l for l in self.logs if l[0]!= "ERROR_TO_RAISE"]),
               'error':len([l for l in self.logs if l[0]== "ERROR"]),
               'warning':len([l for l in self.logs if l[0]== "WARNING"]),
               'info':len([l for l in self.logs if l[0]== "INFO"])}
    
    def df_to_html(self):
        if len(self.dataframe)>0:
            return self.dataframe[self.dataframe['Sévérité']!="ERROR_TO_RAISE"].style.apply(highlight, axis=1).hide_index().render()
        else:
            return ""
        
    def set_dataframe(self):
        self.dataframe = pd.DataFrame({'Sévérité':[v[0] for v in self.logs], 
                                     'Sujet': [v[1] for v in self.logs],
                                     'Description': [v[2] for v in self.logs]
                                    })
        return self.dataframe
        
        
