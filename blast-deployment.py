#!/usr/bin/env python

from pprint import pformat
from execo import Remote, logger, Process, SshProcess, configuration, Put, Host,\
    ChainPut
from execo.log import style
from execo_g5k import get_g5k_sites, get_current_oar_jobs, get_planning,\
    compute_slots, find_free_slot, distribute_hosts, get_jobs_specs, \
    oarsub, get_oar_job_info, wait_oar_job_start, get_oar_job_nodes, \
    get_oar_job_subnets, Deployment, deploy, get_host_site, \
    default_frontend_connection_params
from execo_g5k.planning import show_resources
from execo.time_utils import format_date
from vm5k.utils import hosts_list
from hadoop_g5k import HadoopCluster


default_site = 'reims'
default_n_nodes = 4
default_walltime = '2:00:00'
default_job_name = 'blast_eval'
configuration['color_styles']['step'] = 'on_yellow', 'bold'

def main():
    logger.info('%s\n', style.step(' Launching deployment of a Hadoop'
                'cluster for Blast experimentations '))
    hosts = get_resources(default_site, default_n_nodes, default_walltime, default_job_name)
    hosts = setup_hosts(hosts)
    
    
    


def get_resources(site=None, n_nodes=None, walltime=None, job_name=None):
    """Try to find a running job and reserve resources if needed"""
    logger.info('Looking for a running job on %s',
                style.host(site))
    job_id = None
    running_jobs = get_current_oar_jobs([site])
    for job in running_jobs:
        info = get_oar_job_info(job[0], site)
        if info['name'] == job_name:
            job_id = job[0]
            logger.info('Job %s found !', style.emph(job_id))
            break
    if not job_id:
        logger.info('Performing a new reservation')
        job_id = _make_reservation(site, n_nodes, walltime, job_name)

    logger.info('Waiting for job start ...')
    wait_oar_job_start(job_id, site)
    hosts = get_oar_job_nodes(job_id, site)
    if len(hosts) != n_nodes:
        logger.error('Number of hosts %s in running job does not match wanted'
        ' number of physical nodes %s', len(hosts), n_nodes)
        exit()
    logger.info('Hosts: %s', hosts_list(hosts))
    
    return hosts


def setup_hosts(hosts):
    """ """
    logger.info('Deploying hosts')
    deployed_hosts, _ = deploy(Deployment(hosts=hosts,
                                          env_name="wheezy-x64-prod"))
    hosts = map(lambda x: Host(x),
                sorted(list(deployed_hosts), 
                       key=lambda host: (host.split('.', 1)[0].split('-')[0],
                                         int(host.split('.', 1)[0].split('-')[1]))),)
    Put(hosts, ['/home/lpouilloux/.ssh/']).run()
    logger.info('Creating Hadoop cluster')
    cluster = HadoopCluster(hosts)
    cluster.bootstrap('/home/lpouilloux/public/softs/hadoop-0.20.2.tar.gz')
    cluster.initialize()
    cluster.start()
    logger.info('Copying Hadoop-Blast file')
    ChainPut(hosts, ['/home/lpouilloux/public/softs/BlastProgramAndDB.tar.gz',
                     '/home/lpouilloux/public/softs/Hadoop-Blast.zip']).run()
    logger.info('Extracting archive')
    Remote('unzip -u Hadoop-Blast.zip;', hosts).run()
    Remote('mkdir -p blast && tar -zxvf BlastProgramAndDB.tar.gz -C blast', hosts).run()
    
            
    return cluster

def _make_reservation(site=None, n_nodes=None, walltime=None, job_name=None):
    """ """
    elements = {site: n_nodes}
    logger.detail(pformat(elements))

    planning = get_planning(elements)
    slots = compute_slots(planning, walltime=walltime)
    slot = find_free_slot(slots, elements)
    logger.debug(pformat(slot))
    startdate = slot[0]
    resources = distribute_hosts(slot[2], elements)
    jobs_specs = get_jobs_specs(resources, name=job_name)
    sub, site = jobs_specs[0]
    sub.additional_options = "-t deploy"
    jobs = oarsub(jobs_specs)
    job_id = jobs[0][0]
    logger.info('Job %s will start at %s', style.emph(job_id),
                style.log_header(format_date(startdate)))
    
    return job_id

  
if __name__ == "__main__":
    main()
