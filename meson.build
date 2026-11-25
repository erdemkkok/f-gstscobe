project('gstscope', 'c',
  version : run_command('cat', 'VERSION', check : true).stdout().strip(),
  meson_version : '>= 0.53',
  default_options : ['buildtype=release', 'warning_level=2', 'c_std=gnu99'])

gst_dep = dependency('gstreamer-1.0', version : '>= 1.16')

plugins_dir = get_option('plugindir')
if plugins_dir == ''
  plugins_dir = gst_dep.get_pkgconfig_variable('pluginsdir')
endif

add_project_arguments('-DGSTSCOPE_VERSION="@0@"'.format(meson.project_version()),
  '-DGST_USE_UNSTABLE_API', '-Wno-unused-parameter', language : 'c')

shared_module('gstscope', 'tracer/gstscopetracer.c',
  dependencies : [gst_dep],
  install : true,
  install_dir : plugins_dir)
