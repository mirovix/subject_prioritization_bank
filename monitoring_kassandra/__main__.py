from kassandra.starter.log import log
log.info('>> *** STARTING MONITORING KASSANDRA ***')

from monitoring_kassandra.monitoring_service import main
main()