from django.db import models

# Create your models here.

class Unitelegale(models.Model):
    siren = models.CharField(max_length=70, null=True)
    class Meta:
        indexes = [
            models.Index(
                fields=['siren'],
                name='idx_siren',
            ),
        ]
    dateFin = models.DateField(null=True)
    dateDebut = models.DateField(null=True)
    etatAdministratifUniteLegale = models.CharField(max_length=70, null=True)
    changementEtatAdministratifUniteLegale = models.BooleanField(null=True)
    nomUniteLegale = models.CharField(max_length=70, null=True)
    changementNomUniteLegale = models.BooleanField(null=True)
    nomUsageUniteLegale = models.CharField(max_length=70, null=True)
    changementNomUsageUniteLegale = models.BooleanField(null=True)
    denominationUniteLegale = models.CharField(max_length=70, null=True)
    changementDenominationUniteLegale = models.BooleanField(null=True)
    denominationUsuelle1UniteLegale = models.CharField(max_length=70, null=True)
    denominationUsuelle2UniteLegale = models.CharField(max_length=70, null=True)
    denominationUsuelle3UniteLegale = models.CharField(max_length=70, null=True)
    changementDenominationUsuelleUniteLegale = models.BooleanField(null=True)
    categorieJuridiqueUniteLegale = models.CharField(max_length=70, null=True)
    changementCategorieJuridiqueUniteLegale = models.BooleanField(null=True)
    activitePrincipaleUniteLegale = models.CharField(max_length=70, null=True)
    nomenclatureActivitePrincipaleUniteLegale = models.CharField(max_length=70, null=True)
    changementActivitePrincipaleUniteLegale = models.CharField(max_length=70, null=True)
    nicSiegeUniteLegale = models.CharField(max_length=70, null=True)
    changementNicSiegeUniteLegale = models.BooleanField(null=True)
    economieSocialeSolidaireUniteLegale = models.CharField(max_length=70, null=True)
    changementEconomieSocialeSolidaireUniteLegale = models.BooleanField(null=True)
    caractereEmployeurUniteLegale = models.CharField(max_length=70, null=True)
    changementCaractereEmployeurUniteLegale = models.BooleanField(null=True)
    '''==='''
    statutDiffusionUniteLegale = models.BooleanField(null=True)
    unitePurgeeUniteLegale = models.BooleanField(null=True)
    dateCreationUniteLegale = models.DateField(null=True)
    sigleUniteLegale = models.CharField(max_length=70, null=True)
    sexeUniteLegale = models.CharField(max_length=1,  null=True)
    prenom1UniteLegale = models.CharField(max_length=50,null=True)
    prenom2UniteLegale = models.CharField(max_length=50,null=True)
    prenom3UniteLegale = models.CharField(max_length=50,null=True)
    prenom4UniteLegale = models.CharField(max_length=50,null=True)
    prenomUsuelUniteLegale = models.CharField(max_length=50,null=True)
    pseudonymeUniteLegale = models.CharField(max_length=50,null=True)
    identifiantAssociationUniteLegale = models.CharField(max_length=70, null=True)
    trancheEffectifsUniteLegale = models.CharField(max_length=70, null=True)
    anneeEffectifsUniteLegale = models.CharField(max_length=70, null=True)
    dateDernierTraitementUniteLegale = models.DateField(null=True)
    nombrePeriodesUniteLegale = models.CharField(max_length=70, null=True)
    categorieEntreprise = models.CharField(max_length=70, null=True)
    anneeCategorieEntreprise = models.CharField(max_length=70, null=True)
    