#
# fonctions/outils pour extraire les points de stations des données de vent sur grille
#
# S. Biner, oct 2025
#

# importations

import os

import xarray as xr
import clisops
import xscen as xs
import xclim as xc
from tqdm import tqdm
from dask.diagnostics import ProgressBar


# constantes

f_stations_asos = "/home/biner/exec/1_projets/202509_climato_vent/data/stations_asos/ASOS_preprocessed.zarr.zip"
r_daf = "/home/biner/exec/1_projets/202509_climato_vent/data/daf/series"

def extrait_points_asos_du_mrcc():
    """fonction qui extrait les po,ints des stations ASOS de la grille du MRCC"""

    # boucle sur les années
    
    for annee in range(1995, 2024+1):

        # lecture et mise en forme pour la suite
        sfcwind = xr.open_mfdataset(os.path.join(r_daf, str(annee), "sfcWind_*_se.nc"))
        ds_asos = xr.open_dataset(f_stations_asos, engine="zarr")
        ds_asos = ds_asos.rename(station="site")

        # sfcwind_pt_asos = xc.spatial.subset(sfcwind, method="gridpoint", lon=ds_asos.lon, 
        #                                     lat=ds_asos.lat, add_distance=True, name="station")

        # boucle sur les stations
        l_ds = []
        for ii in tqdm(range(ds_asos.site.size)):
            ds_pt = ds_asos.isel(site=range(ii, ii+1))
            var_pt = xs.spatial.subset(sfcwind, "gridpoint", lon=ds_pt.lon, lat=ds_pt.lat, add_distance=True)
            l_ds.append(var_pt)
        ds_pt = xr.concat(l_ds, dim="site")
        
        # sauvegarde du fichier
        with ProgressBar():
            r_nc = "/home/biner/exec/1_projets/202509_climato_vent/data/daf_stations_asos"
            f_nc = f"daf_extraction_stations_asos_{annee}.nc"
            p_nc = os.path.join(r_nc, f_nc)
            print(f"sauvegarde dans {p_nc}")
            ds_pt.to_netcdf(p_nc)

    print("fin normale")

def extrait_points_asos_era5l(an_debut=1965, an_fin=1978):
    """fonction qui extrait les points des stations ASOS de la grille d'ERA5-Land"""


    # lecture et mise en forme pour la suite
    r_era5l = "/home/biner/exec/1_projets/202509_climato_vent/data/reconstruction_NAM/ECMWF/ERA5-Land"
    uas = xr.open_mfdataset(os.path.join(r_era5l, "1hr", "uas", "uas_1hr_NAM_*zip"), engine="zarr")["uas"]
    vas = xr.open_mfdataset(os.path.join(r_era5l, "1hr", "vas", "vas_1hr_NAM_*zip"), engine="zarr")["vas"]
    sfcwind, bidon  = xc.indicators.atmos.wind_speed_from_vector(uas, vas)
    f_stations_asos = "/home/biner/exec/1_projets/202509_climato_vent/data/stations_asos/ASOS_preprocessed.zarr.zip"
    ds_asos = xr.open_dataset(f_stations_asos, engine="zarr")
    ds_asos = ds_asos.rename(station="site")

        # sfcwind_pt_asos = xc.spatial.subset(sfcwind, method="gridpoint", lon=ds_asos.lon, 
        #                                     lat=ds_asos.lat, add_distance=True, name="station")

    # boucle sur les années
    for annee in range(an_debut, an_fin + 1):

        sfcwind_r = sfcwind.sel(time=str(annee))
        # boucle sur les stations
        l_ds = []
        for ii in tqdm(range(ds_asos.site.size)):
            ds_pt = ds_asos.isel(site=range(ii, ii+1))
            var_pt = xs.spatial.subset(sfcwind_r, "gridpoint", lon=ds_pt.lon, lat=ds_pt.lat, add_distance=True)
            l_ds.append(var_pt)
        ds_pt = xr.concat(l_ds, dim="site")
        
        # sauvegarde du fichier
        with ProgressBar():
            r_nc = "/home/biner/exec/1_projets/202509_climato_vent/data/era5l_stations_asos"
            f_nc = f"era5l_extraction_stations_asos_{annee}.nc"
            p_nc = os.path.join(r_nc, f_nc)
            print(f"sauvegarde dans {p_nc}")
            ds_pt.to_netcdf(p_nc)

    print("fin normale")

def extrait_points_mats_du_mrcc():
    """fonction qui extrait les points des mats HQ de la grille du MRCC"""

    rep_data_mrcc = "/home/biner/exec/1_projets/202509_climato_vent/data/data_olivier/diagnostics_exact/daf/100m/hourly"

    # lecture des données des mats et mise en forme
    f_mats_hq = "/exec/biner/1_projets/202509_climato_vent/data/stations_mats_hq/traitees/data_mat_tous.nc"
    ds_mats = xr.open_dataset(f_mats_hq)
    ds_mats = ds_mats.rename(parc_mat="site")

    # boucle sur les années
    for annee in range(2013, 2024+1):

        # lecture et mise en forme pour la suite
        vent_daf = xr.open_mfdataset(os.path.join(rep_data_mrcc, "wind", f"U_daf_{annee}*.nc"))

        # boucle sur les stations
        l_ds = []
        for ii in tqdm(range(ds_mats.site.size)):
            ds_pt = ds_mats.isel(site=range(ii, ii+1))
            var_pt = xs.spatial.subset(vent_daf, "gridpoint", lon=ds_pt.lon, lat=ds_pt.lat, add_distance=True)
            l_ds.append(var_pt)
        ds_pt = xr.concat(l_ds, dim="site")
        
        # sauvegarde du fichier
        with ProgressBar():
            r_nc = "/home/biner/exec/1_projets/202509_climato_vent/data/daf_mats_hq"
            f_nc = f"daf_vent_100m_extraction_mats_hq_{annee}.nc"
            p_nc = os.path.join(r_nc, f_nc)
            print(f"sauvegarde dans {p_nc}")
            ds_pt.to_netcdf(p_nc)

        

    print("fin normale")

def extrait_points_mats_hq_era5(an_debut=2008, an_fin=2024):
    """fonction qui extrait les points des mats hq d'ERA5"""

    # lecture des données d'era5 et calcul de la vitesse du vent
    r_era5 = "/home/biner/exec/1_projets/202509_climato_vent/data/reconstruction_NAM/ECMWF/ERA5"
    uu = xr.open_mfdataset(os.path.join(r_era5, "1hr", "ua100m", "ua100m_1hr_NAM_*zip"), engine="zarr")["ua100m"]
    vv = xr.open_mfdataset(os.path.join(r_era5, "1hr", "va100m", "va100m_1hr_NAM_*zip"), engine="zarr")["va100m"]
    sfcwind, bidon  = xc.indicators.atmos.wind_speed_from_vector(uu, vv)

    # lecture des données des mats et mise en forme
    f_mats_hq = "/exec/biner/1_projets/202509_climato_vent/data/stations_mats_hq/traitees/data_mat_tous.nc"
    ds_mats = xr.open_dataset(f_mats_hq)
    ds_mats = ds_mats.rename(parc_mat="site")

        # sfcwind_pt_asos = xc.spatial.subset(sfcwind, method="gridpoint", lon=ds_asos.lon, 
        #                                     lat=ds_asos.lat, add_distance=True, name="station")

    # boucle sur les années
    for annee in range(an_debut, an_fin + 1):

        sfcwind_r = sfcwind.sel(time=str(annee))
        # boucle sur les stations
        l_ds = []
        for ii in tqdm(range(ds_mats.site.size)):
            ds_pt = ds_mats.isel(site=range(ii, ii+1))
            var_pt = xs.spatial.subset(sfcwind_r, "gridpoint", lon=ds_pt.lon, lat=ds_pt.lat, add_distance=True)
            l_ds.append(var_pt)
        ds_pt = xr.concat(l_ds, dim="site")
        
        # sauvegarde du fichier
        with ProgressBar():
            r_nc = "/home/biner/exec/1_projets/202509_climato_vent/data/era5_mats_hq"
            f_nc = f"era5_extraction_mats_hq_{annee}.nc"
            p_nc = os.path.join(r_nc, f_nc)
            print(f"sauvegarde dans {p_nc}")
            ds_pt.to_netcdf(p_nc)

    print("fin normale")







def main():
    # extrait_points_asos_du_mrcc()
    # extrait_points_mats_du_mrcc()
    extrait_points_mats_hq_era5()


if __name__ == "__main__":
    main()
