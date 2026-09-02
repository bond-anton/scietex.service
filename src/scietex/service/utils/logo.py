"""ASCII logo printer for ``scietex.service``.

Provides :data:`LOGO` — a template string with placeholders for
``{service_name}``, ``{version}``, and ``{scietex_version}`` — and
:func:`print_scietex_logo` to render and print it.
"""

from ..version import __version__

LOGO = """

          ########+                                                            
          #########+                                                           
          ##########-         Service: {service_name}
          ###########-        Version: {version}
           .##########-                      
              .+#######-      
     +#+..        .#####-                                                      
   -##########.      .+##-                                                     
 -#################+-           
 ####################         Powered by scietex.service v{scietex_version}
  .############-.    .-##-      
    .####+.       .#####-     (c) ООО "Научные технологии и сервис"
               -#######-      https://scietex.ru
           .##########-                     
          ###########-                      
          ##########+                                                  
          ##########                                                           
          #########                                                            
 
"""


def print_scietex_logo(service_name: str, version: str) -> None:
    """Print the Scietex Service logo with service-specific details.

    Args:
        service_name: Name of the running service.
        version: Version string of the running service.

    The scietex.service version is resolved automatically from
    ``..version.__version__`` at call time.
    """
    print(LOGO.format(service_name=service_name, version=version, scietex_version=__version__))
