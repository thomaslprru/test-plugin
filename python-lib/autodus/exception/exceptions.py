class ExceptionImproved(Exception):
    base_link = "https://edh0labue-dku.prod.gcp.groupement.systeme-u.fr/projects/UIRIS/wiki/1/Macro%20de%20normalisation"
    
    def __init__(self, message,anchor_value):
        super().__init__(message+ "Plus d'informations : {:}".format(self.base_link+"#"+anchor_value))
       
    
# Ce fichier itère les différentes exceptions du projet de variabilisation
class NormalisationConnexionSQLException(ExceptionImproved):
    pass
        
class TableAIndustrialiserException(ExceptionImproved):
    pass

class DereferencementVariableException(ExceptionImproved):
    pass

class NormalisationRecetteSqlException(ExceptionImproved):
    pass

class ProjectAccessRightException(ExceptionImproved):
    pass

class SaveException(ExceptionImproved):
    pass