"""Static parameter definitions."""

from numpy import linspace


raytrace_resolution = 64
raytrace_kwargs = dict(
    solver_rtol=1.0e-6,
    solver_atol=1.0e-6,
    nintegrate=50,
    shell_smoothing_length=0.0,
    optically_thin=True,
    nsample_z=100,
    interpolate_temperature=True,
)

image_times = [-0.03, -0.02, -0.01, 0.0]
image_hnu = [12.0, 20.0, 35.0, 50.0]
image_los = [(0.0, 0.0), (90.0, 78.0), (90.0, 0.0)]

static_shot_params = dict(
    R0=100.0,
    E0=150.0e-3,
    Mhs=5.0,
    conduction_model_eta=0.01,
    #  R0Shell = 200.,
    dRShell=100.0,  # / 2.5133,
    MShell=100.0,
    infalling_adiabat=2.0 * 1.07886e10 * 766.0 / 909.0,
    Vi=300.0,
    A=2.515,
    cv=1.5,
    Rhat0=100.0,
    shell_model="betti_prl15",
    fusion_model_sv="fowler_caughlin",
    infalling_model="betti_prl15",
    hotspot_model="betti_prl15",
    conduction_mult=1.5,
    ablation_cv=3.0,
    shape_model="thinshell",
    shape_model_lmax=6,
    include_stopping=True,
    include_shell_opacity=False,
    include_hotspot_opacity=False,
    include_conduction=True,
    conduction_model_conductivity="spitzer",
    include_radiation=True,
    radiation_model="bremms",
    radiation_model_include_radial_integral=True,
    radiation_mult=1.0,
    include_ei=False,
    single_temperature=True,
    include_ablation=True,
    include_hs_opac=True,
    #  limit_shell_mass=True,
    betti_prl15_initial=True,
    betti_prl15_A0=10.0,
    betti_prl15_trans=True,
    betti_prl15_trans_points=[[1.4, 20], [2.0, 32.0], [1.6, 34.0], [1.0, 28.5]],
    # Run options.
    plot=False,
    verbose=False,
    stop_on_error=False,
    tmax=4.0,
    Tmin=0.0,
    timestep=0.005,
    timestep_gradient=(0.1 - 0.005) / 99.0,
    timestep_Roffset=1.0,
    fusion_cutoff=0.01,
    postp_image_decompose=True,
    postp_image_decompose_lmax=6,
    postp_timeseries_vars=["R", "P", "T", "fusion_power", "image_moments"],
    postp_timeseries_times=linspace(-0.3, 0.0, 61),
    postp_image_def=[
        dict(
            image_time=image_times,
            image_photon_energy=image_hnu,
            image_xpix=linspace(-63.0, 63.0, raytrace_resolution),
            image_ypix=linspace(-63.0, 63.0, raytrace_resolution),
            image_view=v,
            raytrace_kwargs=raytrace_kwargs,
        )
        for v in image_los
    ],
    betti_prl15_trans_u=0.5,
    betti_prl15_trans_v=0.5,
)

for mode in [(1, 0), (2, 1), (4, 3)]:
    key = "shape_model_initial_modes:({0},{1})".format(mode[0], mode[1])
    static_shot_params[key] = 0.0
