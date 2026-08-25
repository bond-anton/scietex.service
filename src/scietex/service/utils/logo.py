"""
Module providing ASCII logo for use in std output.
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
    """Print formatted Scietex Service Logo."""
    print(LOGO.format(service_name=service_name, version=version, scietex_version=__version__))
