select 
    CONCAT(major, minor) as major_minor
    ,major
    ,minor
    ,{{ xtrim('prop_name') }}
    ,plat_name
    ,plat_lot
    ,plat_block
    ,"range"
    ,township
    ,"section"
    ,quarter_section
    ,prop_type
    ,area
    ,sub_area
    ,spec_area
    ,spec_sub_area
    ,district_name
    ,levy_code
    ,current_zoning
    ,h_b_u_as_if_vacant
    ,h_b_u_as_improved
    ,present_use
    ,sq_ft_lot
    ,water_system
    ,sewer_system
    ,"access"
    ,topography
    ,street_surface
    ,restrictive_sz_shape
    ,inadequate_parking
    ,pcnt_unusable
    ,unbuildable
    ,mt_rainier
    ,olympics
    ,cascades
    ,territorial
    ,seattle_skyline
    ,puget_sound
    ,lake_washington
    ,lake_sammamish
    ,small_lake_river_creek
    ,other_view
    ,wfnt_location
    ,wfnt_footage
    ,wfnt_bank
    ,wfnt_poor_quality
    ,wfnt_restricted_access
    ,wfnt_access_rights
    ,wfnt_proximity_influence
    ,tideland_shoreland
    ,lot_depth_factor
    ,traffic_noise
    ,airport_noise
    ,power_lines
    ,other_nuisances
    ,nbr_bldg_sites
    ,contamination
    ,d_n_r_lease
    ,adjacent_golf_fairway
    ,adjacent_greenbelt
    ,historic_site
    ,current_use_designation
    ,native_growth_prot_esmt
    ,easements
    ,other_designation
    ,deed_restrictions
    ,development_rights_purch
    ,coal_mine_hazard
    ,critical_drainage
    ,erosion_hazard
    ,landfill_buffer
    ,hundred_yr_flood_plain
    ,seismic_hazard
    ,landslide_hazard
    ,steep_slope_hazard
    ,stream
    ,wetland
    ,species_of_concern
    ,sensitive_area_tract
    ,water_problems
    ,transp_concurrency
    ,other_problems
from {{ source('src', 'raw_parcel') }}
