#!/usr/bin/env python

import sys
from pprint import pformat
from execo import Remote, logger, Process, SshProcess, configuration, Put, Host,\
    ChainPut, default_connection_params, TaktukRemote
from execo.log import style
from execo_g5k import get_g5k_sites, get_current_oar_jobs, get_planning,\
    compute_slots, find_free_slot, distribute_hosts, get_jobs_specs, \
    oarsub, get_oar_job_info, wait_oar_job_start, get_oar_job_nodes, \
    get_oar_job_subnets, Deployment, deploy, get_host_site, \
    default_frontend_connection_params
from execo_g5k.planning import show_resources, get_job_by_name
#from execo_g5k.utils import hosts_list, g5k_args_parser
from execo.time_utils import format_date
from vm5k.utils import hosts_list
from hadoop_g5k import HadoopCluster, HadoopJarJob

default_site = 'nancy'
default_n_nodes = 29
default_walltime = '2:00:00'
default_job_name = 'blast_eval'
storage = '/data/lpouilloux_762670/'
blast_file = 'BlastProgramAndDB.tar.gz'
gz_fasta_file = 'GRID5000.fasta.gz'
jar_file = 'blast-hadoop-quotedArgs.jar'
split_file = 'split_fasta.pl'
blast_db = gz_fasta_file[0:-3] + '_DB_blastp.tar.gz'
configuration['color_styles']['step'] = 'on_yellow', 'bold'
default_connection_params['user'] = 'root'

blast_configuration = {'run_dir': '/tmp/blast',
                       'headers': 'qseqid sseqid evalue pident bitscore qstart qend qlen sstart send sle',
                       'e_value': 1e-5,
                       'other_args': "-seg yes -soft_masking true"}


def main():
    logger.info('%s\n', style.step(' Launching deployment of a Hadoop'
                'cluster for Blast experimentations '))
    hosts = get_resources(default_site, default_n_nodes, default_walltime, default_job_name)
    cluster = setup_hadoop_cluster(hosts)
    try:
        install_blast(cluster.hosts)
        
        prepare_data(cluster)

        job = HadoopJarJob(storage + jar_file, 
                           params=[blast_db, '/usr/bin/blastp', '/tmp/hadoop-blastpAaA/', 'db',
                                   gz_fasta_file[0:-3],  gz_fasta_file[0:-3] + '_split ',
                                   gz_fasta_file[0:-3] + '_output',
                                   '-query #_INPUTFILE_# -evalue 1e-5 -max_target_seqs 5000 ' +
                                   '-outfmt "6 qseqid sseqid evalue pident bitscore qstart qend qlen sstart send slen"'])
        logger.setLevel('DEBUG')
        cluster.execute_job(job)
    finally:
        logger.setLevel('INFO')
#        cluster.stop()

def install_blast(hosts):
    """ """
    apt_blast = TaktukRemote('apt-get update && apt-get install -y ncbi-blast+',
                             hosts).run()
    return apt_blast.ok

#    cp_blast = ChainPut(hosts, [blast_file]).run()
#    if not cp_blast.ok:
#        logger.error('Trouble with BlastProgramAndDB copy ')
#        exit()
#    Remote('mkdir -p /tmp/blast/ ; tar -zxvf BlastProgramAndDB.tar.gz -C /tmp/blast', hosts).run()
    
    
def prepare_data(cluster):
    """ """
    mkdir = SshProcess('mkdir -p /tmp/data', cluster.master).run()
    files = [storage + gz_fasta_file,
             storage + split_file] 
    logger.info('Copying files %s', pformat(files))
    copy_files = Put(cluster.master, files, remote_location='/tmp/data/').run()
    
    fasta_exist = SshProcess('ls -l /tmp/data/' + gz_fasta_file[0:-3],
                             cluster.master,
                             nolog_exit_code=True).run()
    if not fasta_exist.ok:
        logger.info('Extracting %s', gz_fasta_file)
        extract_fasta = SshProcess('cd /tmp/data ; gzip -f -d GRID5000.fasta.gz',
                                   cluster.master).run()
    in_file = '/tmp/data/' + gz_fasta_file[0:-3]
    out_file = '/tmp/data/db/' + gz_fasta_file[0:-3]

    db_exist = SshProcess('ls -l /tmp/data/' + blast_db, cluster.master,
                             nolog_exit_code=True).run()
    if not db_exist.ok:
        logger.info('Creating blast DB from %s to %s ', in_file, out_file)
        makedb = SshProcess('mkdir -p /tmp/data/db ; makeblastdb -in ' + in_file +
                            ' -out ' + out_file + ' -dbtype prot', cluster.master).run()
        gzdb = SshProcess('cd /tmp/data ; tar -zcf ' + gz_fasta_file[0:-3] + 
                          '_DB_blastp.tar.gz db', cluster.master).run()
    split_fasta = SshProcess('perl /tmp/data/' + split_file + ' ' + in_file + ' ' +
                             str(len(cluster.hosts)), cluster.master).run()

    cluster.execute("fs -put " + in_file + '_split ' + gz_fasta_file[0:-3] + '_split')
    cluster.execute("fs -put /tmp/data/" + gz_fasta_file[0:-3] + "_DB_blastp.tar.gz "
                    + gz_fasta_file[0:-3] + "_DB_blastp.tar.gz ")
     
def setup_hadoop_cluster(hosts):
    """ """
    logger.info('Deploying hosts')
    deployed_hosts, _ = deploy(Deployment(hosts=hosts,
                                          env_name="wheezy-x64-base"))
    hosts = map(lambda x: Host(x),
                sorted(list(deployed_hosts), 
                       key=lambda host: (host.split('.', 1)[0].split('-')[0],
                                         int(host.split('.', 1)[0].split('-')[1]))),)
    Put(hosts, ['/home/lpouilloux/.ssh/']).run()
    TaktukRemote('apt-get update --fix-missing', hosts).run()
    logger.info('Creating Hadoop cluster')
    cluster = HadoopCluster(hosts)
    hadoop_exist = SshProcess('ls -l /tmp/hadoop/', cluster.master,
                              nolog_exit_code=True).run()
    if not hadoop_exist.ok:
        cluster.bootstrap('/home/lpouilloux/public/softs/hadoop-0.20.2.tar.gz')
    cluster.initialize()    
    cluster.start()
                
    return cluster

def get_resources(site=None, n_nodes=None, walltime=None, job_name=None):
    """Try to find a running job and reserve resources if needed"""
    logger.info('Looking for a running job ...')
    job_id, site = get_job_by_name(job_name)
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
    sub.reservation_date = startdate
    sub.walltime = walltime
    jobs = oarsub(jobs_specs)
    job_id = jobs[0][0]
    logger.info('Job %s will start at %s', style.emph(job_id),
                style.log_header(format_date(startdate)))
    
    return job_id

  
if __name__ == "__main__":
    main()
