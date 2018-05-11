# import atexit
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnectNoSSL#, Disconnect
import time
import json
from datetime import timedelta
import logging
import voluptuous as vol
 
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import (
    ATTR_ATTRIBUTION,CONF_TYPE)
from homeassistant.helpers.entity import Entity
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.event import track_time_interval
import homeassistant.util.dt as dt_util
 
 
_LOGGER = logging.getLogger(__name__)
 
TIME_BETWEEN_UPDATES = timedelta(seconds=60)

CONF_USERNAME = "username"
CONF_PASSWORD = "password"
CONF_VCHOST = "vchost"
CONF_PORT = "port" 
CONF_DATASTORE = "datastore"
CONF_ESXI = "esxi"
CONF_VM = "vm"
CONF_METRIC = "metric"

DATASTORE_DEFAULT="capacity"

CONF_ATTRIBUTION="Powered by Syjjx"

REQUIREMENTS = ['pyvmomi==6.7']

DATASTORE = {
    "freePercent": ["datastore_freePercent", "存储剩余容量百分比", "mdi:harddisk", "%"]
}
ESXI = {
    "if_in": ["esxi_net_if_in", "下载速度", "mdi:server-network", "mbps"],
    "if_out": ["esxi_net_if_out", "上传速度", "mdi:server-network", "mbps"],
    "memory": ["esxi_memory_freePercent", "内存使用率", "mdi:memory", "%"],
    "cpu": ["esxi_cpu_usage", "CPU使用率", "mdi:memory", "%"],
    "uptime": ["esxi_uptime", "开机时间", "mdi:clock", ""],      
} 
VM = {
    "if_in": ["vm_net_if_in", "下载速度", "mdi:server-network", "mbps"],
    "if_out": ["vm_net_if_out", "上传速度", "mdi:server-network", "mbps"],
    "io_write": ["vm_datastore_io_write_bytes", "写流量", "mdi:harddisk", "MB/s"],
    "io_read": ["vm_datastore_io_read_bytes", "读流量", "mdi:harddisk", "MB/s"],
    "memory": ["vm_memory_freePercent", "内存使用率", "mdi:memory", "%"],
    "cpu": ["vm_cpu_usage", "CPU使用率", "mdi:memory", "%"],
    "uptime": ["vm_uptime", "开机时间", "mdi:clock", ""]
} 

 
 
PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_USERNAME): cv.string,
    vol.Required(CONF_PASSWORD): cv.string,
    vol.Required(CONF_VCHOST): cv.string,
    vol.Optional(CONF_PORT,default=443): cv.string,    
    vol.Optional(CONF_DATASTORE):
        vol.All(cv.ensure_list, [vol.Schema({
            vol.Required(CONF_TYPE): cv.string,
            vol.Optional(CONF_METRIC): cv.ensure_list,
        })]),
    vol.Optional(CONF_ESXI):
        vol.All(cv.ensure_list, [vol.Schema({
            vol.Required(CONF_TYPE): cv.string,
            vol.Optional(CONF_METRIC): cv.ensure_list,
        })]),      
    vol.Optional(CONF_VM):
        vol.All(cv.ensure_list, [vol.Schema({
            vol.Required(CONF_TYPE): cv.string,
            vol.Optional(CONF_METRIC): cv.ensure_list,
        })]),                
})
 
 
def setup_platform(hass, config, add_devices, discovery_info=None):
 

    username = config.get(CONF_USERNAME)
    password = config.get(CONF_PASSWORD)
    vchost = config.get(CONF_VCHOST)
    port = config.get(CONF_PORT)
 
    dev = []
    datastore_names = []
    esxi_names = []
    vm_names = []
    client = Hello_Esxi(vchost,username,password,port=port)
    if client.vcenter_status[0] == True: 
        json_vcenter_status=json.loads(client.vcenter_status[1])
        _LOGGER.error(client.vcenter_status[1])
        for datastore in config[CONF_DATASTORE]:
            if datastore[CONF_TYPE] in json_vcenter_status['datastore']:               
                datastore_names.append(datastore[CONF_TYPE])
                client.set_datastore_names(datastore_names)
                if datastore.get(CONF_METRIC) !=None:            
                    for key in datastore[CONF_METRIC]:
                        dev.append(EsxiSensor([datastore[CONF_TYPE],datastore[CONF_TYPE]+'_'+DATASTORE[key][0],'datastore'],DATASTORE[key],client)) 
                else:
                    for key in DATASTORE:  
                        dev.append(EsxiSensor([datastore[CONF_TYPE],datastore[CONF_TYPE]+'_'+DATASTORE[key][0],'datastore'],DATASTORE[key],client))               
            else:
                _LOGGER.error("You don't have DATASTORE named {} !".format(datastore[CONF_TYPE]))   
        for esxi in config[CONF_ESXI]:
            if esxi[CONF_TYPE] in json_vcenter_status['esxi']:
                esxi_names.append(esxi[CONF_TYPE])
                client.set_esxi_names(esxi_names)
                if esxi.get(CONF_METRIC) !=None:            
                    for key in esxi[CONF_METRIC]:
                        dev.append(EsxiSensor([esxi[CONF_TYPE],esxi[CONF_TYPE]+'_'+ESXI[key][0],'esxi'],ESXI[key],client)) 
                else:
                    for key in ESXI: 
                        dev.append(EsxiSensor([esxi[CONF_TYPE],esxi[CONF_TYPE]+'_'+ESXI[key][0],'esxi'],ESXI[key],client))  
            else:
                _LOGGER.error("You don't have ESXI named {} !".format(esxi[CONF_TYPE]))             
        for vm in config[CONF_VM]:
            if vm[CONF_TYPE] in json_vcenter_status['vm']:
                vm_names.append(vm[CONF_TYPE])
                client.set_vm_names(vm_names)
                if vm.get(CONF_METRIC) !=None:            
                    for key in vm[CONF_METRIC]:
                        # _LOGGER.error(vm[CONF_TYPE]+'_'+VM[key][0])
                        dev.append(EsxiSensor([vm[CONF_TYPE],vm[CONF_TYPE]+'_'+VM[key][0],'vm'],VM[key],client)) 
                else:
                    for key in VM:
                        # _LOGGER.error(vm[CONF_TYPE]+'_'+VM[key][0])  
                        dev.append(EsxiSensor([vm[CONF_TYPE],vm[CONF_TYPE]+'_'+VM[key][0],'vm'],VM[key],client))  
            else:
                _LOGGER.error("You don't have VM named {} !".format(vm[CONF_TYPE]))      

        client.start(hass) 
        add_devices(dev, True)
    else:
        _LOGGER.error(client.vcenter_status[1])
 
 
class EsxiSensor(Entity):
 
    def __init__(self,name,option,data):
        """初始化."""
        self._interval=60
        self._data = data
        self._object_id = name
        self._friendly_name = 'null'
        self._icon = option[2]
        self._unit_of_measurement = option[3]
        self.attributes={ATTR_ATTRIBUTION: CONF_ATTRIBUTION}
        self._type = option
        self._state = None
        self._updatetime = None
 


    @property
    def name(self):
        """返回实体的名字."""
        return self._object_id[1]
 
    @property
    def registry_name(self):
        """返回实体的friendly_name属性."""
        return self._friendly_name
 
    @property
    def state(self):
        """返回当前的状态."""
        return self._state
 
    @property
    def icon(self):
        """返回icon属性."""
        return self._icon
 
    @property
    def unit_of_measurement(self):
        """返回unit_of_measuremeng属性."""
        return self._unit_of_measurement
 
    @property
    def device_state_attributes(self):
        """设置其它一些属性值."""
        if self._state is not None:
            return self.attributes
 
    def update(self):
        
        if self._object_id[2] == 'datastore':
            self._friendly_name = self._object_id[0]
            self._state = round((100-json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]]),2)
            self.attributes['容量']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["datastore_capacity"]/1073741824),2))+'GB'
            self.attributes['已用']=str(round(((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["datastore_capacity"]
                                                                -json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["datastore_free"])/1073741824),2))+'GB'
 
        elif self._object_id[2] == 'esxi':
            self._friendly_name = self._object_id[1]            
            if self._type[0] == 'esxi_memory_freePercent':
                self._state = round((100-json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]]),2)
                self.attributes['内存容量']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["esxi_memory_capacity"]/1073741824),2))+'GB'
                self.attributes['已用内存']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["esxi_memory_usage"]/1073741824),2))+'GB'
            elif self._type[0] == 'esxi_net_if_in' or self._type[0] == 'esxi_net_if_out':
                self._state = round(((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])/1024/1024),2)
            elif self._type[0] == 'esxi_cpu_usage':
                self._state = round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]]),2)
            else:
                # _LOGGER.error(json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])
                self._state = timedelta(seconds=json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])
        elif self._object_id[2] == 'vm':
            self._friendly_name = self._object_id[1]  
            if json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["powerState"] != "poweredOn":
                self._state = 0      
            elif self._type[0] == 'vm_memory_freePercent':
                self._state = round((100-json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]]),2)
                self.attributes['内存容量']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["vm_memory_capacity"]/1073741824),2))+'GB'
                self.attributes['已用内存']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["vm_memory_usage"]/1073741824),2))+'GB'                
            elif self._type[0] == 'vm_net_if_in' or self._type[0] == 'vm_net_if_out':  
                self._state = round(((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])/1024/1024),2)
            elif self._type[0] == 'vm_cpu_usage':
                self._state = round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]]),2)
            elif self._type[0] == 'vm_datastore_io_write_bytes':
                self._state = round(((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])/1024/1024),2)
                self.attributes['写延迟']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["vm_datastore_io_write_latency"]),2))+'ms'
                self.attributes['写IOPS']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["vm_datastore_io_write_numbers"]),2))                                
            elif self._type[0] == 'vm_datastore_io_read_bytes':
                self._state = round(((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])/1024/1024),2)
                self.attributes['读延迟']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["vm_datastore_io_read_latency"]),2))+'ms'
                self.attributes['读IOPS']=str(round((json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]]["vm_datastore_io_read_numbers"]),2))
            else:
                self._state = timedelta(seconds=json.loads(self._data.vcenter_info)[self._object_id[2]][self._object_id[0]][self._type[0]])



class Hello_Esxi():

    def __init__(self,vchost,username,password,port=443):
        self._vcenter_status={"datastore":[],"esxi":[],"vm":[]}
        self._vchost=vchost
        self._username=username
        self._password=password
        self._port=port
        self._payload=[]
        self._interval=60
        self._data={"datastore":{},"esxi":{},"vm":{}}
        self.success,self.msg=self.hello_vcenter()
        

    def start(self,hass):
        
        if self.success == True:            
            self.run(dt_util.now())
            # 每隔TIME_BETWEEN_UPDATES，调用一次run(),
            track_time_interval(hass, self.run, TIME_BETWEEN_UPDATES)
        else:
            _LOGGER.error(self.msg)        
    
    def set_datastore_names(self,value):
        self._datastore_names=value

    def set_esxi_names(self,value):
        self._esxi_names=value

    def set_vm_names(self,value):
        self._vm_names=value

    def hello_vcenter(self):
        try:
            self.si = SmartConnectNoSSL(
                host=self._vchost,
                user=self._username,
                pwd=self._password,
                port=self._port)
            hello_content = self.si.RetrieveContent()
            for datacenter in hello_content.rootFolder.childEntity:
                for ds in datacenter.datastore:
                    self._vcenter_status['datastore'].append(ds.name)
                if hasattr(datacenter.hostFolder, 'childEntity'):
                    hostFolder = datacenter.hostFolder
                    computeResourceList = []
                    computeResourceList = self._getComputeResource(hostFolder,computeResourceList)
                    for computeResource in computeResourceList:
                        for host in computeResource.host:
                            self._vcenter_status['esxi'].append(host.name)
            obj = hello_content.viewManager.CreateContainerView(hello_content.rootFolder, [vim.VirtualMachine], True)
            for vm in obj.view:
                self._vcenter_status['vm'].append(vm.name)
            return True, json.dumps(self._vcenter_status,indent=4)
        except vmodl.MethodFault as error:
            return False, error.msg
        except Exception as e:
            return False, str(e)

    @property
    def vcenter_status(self):        
        return self.success,self.msg

    @property
    def vcenter_info(self):
        return json.dumps(self._data,indent=4)

    def _add_data(self,Resource,name,value):
        # data = {"endpoint":'S_Vcenter',"metric":metric,"timestamp":self.ts,"step":self._interval,"value":value,"counterType":conterType,"tags":tags}
        self._data[Resource][name] = value
        self._payload.append(self._data)


    def _DatastoreInformation(self,datastore,datacenter_name):
        try:
            summary = datastore.summary
            name = summary.name
            TYPE = summary.type

            tags = {"datacenter":datacenter_name,"datastore":name,"type":TYPE}

            capacity = summary.capacity
            # self._add_data("datastore_capacity",capacity,"GAUGE",tags)

            freeSpace = summary.freeSpace
            # self._add_data("datastore_free",freeSpace,"GAUGE",tags)

            freeSpacePercentage = (float(freeSpace) / capacity) * 100
            # self._add_data("datastore_freePercent",freeSpacePercentage,"GAUGE",tags)
            value={"datastore_capacity":capacity,"datastore_free":freeSpace,"datastore_freePercent":freeSpacePercentage}
            self._add_data("datastore",name,value)

            
        except Exception as error:
            _LOGGER.error( "Unable to access summary for datastore: ", datastore.name)
            _LOGGER.error( error)
            pass

    def _getComputeResource(self,Folder,computeResourceList):
        if hasattr(Folder, 'childEntity'):
            for computeResource in Folder.childEntity:
               self._getComputeResource(computeResource,computeResourceList)
        else:
            computeResourceList.append(Folder)
        return computeResourceList


    def _ComputeResourceInformation(self,computeResource,datacenter_name,content,perf_dict,vchtime,interval):
        try:
            hostList = computeResource.host
            computeResource_name = computeResource.name
            for host in hostList:
                if (host.name in self._esxi_names) or (len(self._esxi_names) == 0):
                    self._HostInformation(host,datacenter_name,computeResource_name,content,perf_dict,vchtime,interval)
        except Exception as error:
            _LOGGER.error( "Unable to access information for compute resource: ", computeResource.name)
            _LOGGER.error( error)
            pass

    def _HostInformation(self,host,datacenter_name,computeResource_name,content,perf_dict,vchtime,interval):
        try:
            statInt = interval/20
            summary = host.summary
            stats = summary.quickStats
            hardware = host.hardware

            tags = "datacenter=" + datacenter_name + ",cluster_name=" + computeResource_name + ",host=" + host.name

            uptime = stats.uptime

            cpuUsage = 100 * 1000 * 1000 * float(stats.overallCpuUsage) / float(hardware.cpuInfo.numCpuCores * hardware.cpuInfo.hz)

            memoryCapacity = hardware.memorySize

            memoryUsage = stats.overallMemoryUsage * 1024 * 1024

            freeMemoryPercentage = 100 - (
                (float(memoryUsage) / memoryCapacity) * 100
            )

            statNetworkTx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.transmitted.average')), "", host, interval)     
            networkTx = (float(sum(statNetworkTx[0].value[0].value) * 8 * 1024) / statInt)
            
            statNetworkRx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.received.average')), "", host, interval)
            networkRx = (float(sum(statNetworkRx[0].value[0].value) * 8 * 1024) / statInt)

            value={"esxi_uptime":uptime,"esxi_cpu_usage":cpuUsage,"esxi_memory_capacity":memoryCapacity,"esxi_memory_usage":memoryUsage,
                    "esxi_memory_freePercent":freeMemoryPercentage,"esxi_net_if_out":networkTx,"esxi_net_if_in":networkRx}
            self._add_data("esxi",host.name,value)
        except Exception as error:
            _LOGGER.error( "Unable to access information for host: ", host.name)
            _LOGGER.error( error)
            pass

    def _BuildQuery(self,content, vchtime, counterId, instance, entity, interval):
        perfManager = content.perfManager
        metricId = vim.PerformanceManager.MetricId(counterId=counterId, instance=instance)
        startTime = vchtime - timedelta(seconds=(interval + 60))
        endTime = vchtime - timedelta(seconds=60)
        query = vim.PerformanceManager.QuerySpec(intervalId=20, entity=entity, metricId=[metricId], startTime=startTime,
                                                 endTime=endTime)
        perfResults = perfManager.QueryPerf(querySpec=[query])
        if perfResults:
            return perfResults
        else:
            return False

    def _perf_id(self,perf_dict, counter_name):
        counter_key = perf_dict[counter_name]
        return counter_key

    def _VmInfo(self,vm,content,vchtime,interval,perf_dict,tags):
        try:
            statInt = interval/20
            summary = vm.summary
            stats = summary.quickStats
            
            uptime = stats.uptimeSeconds
            
            cpuUsage = 100 * float(stats.overallCpuUsage)/float(summary.runtime.maxCpuUsage)
            
            memoryUsage = stats.guestMemoryUsage * 1024 * 1024
            
            memoryCapacity = summary.runtime.maxMemoryUsage * 1024 * 1024
            
            freeMemoryPercentage = 100 - (
                (float(memoryUsage) / memoryCapacity) * 100
            )
            
            statDatastoreRead = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.read.average')),"*", vm, interval)
            DatastoreRead = (float(sum(statDatastoreRead[0].value[0].value) * 1024) / statInt)
            
            statDatastoreWrite = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.write.average')),"*", vm, interval)
            DatastoreWrite = (float(sum(statDatastoreWrite[0].value[0].value) * 1024) / statInt)
            
            statDatastoreIoRead = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.numberReadAveraged.average')),"*", vm, interval)
            DatastoreIoRead = (float(sum(statDatastoreIoRead[0].value[0].value)) / statInt)
            
            statDatastoreIoWrite = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.numberWriteAveraged.average')),"*", vm, interval)
            DatastoreIoWrite = (float(sum(statDatastoreIoWrite[0].value[0].value)) / statInt)
            
            statDatastoreLatRead = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.totalReadLatency.average')), "*", vm, interval)
            DatastoreLatRead = (float(sum(statDatastoreLatRead[0].value[0].value)) / statInt)

            statDatastoreLatWrite = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.totalWriteLatency.average')), "*", vm, interval)
            DatastoreLatWrite = (float(sum(statDatastoreLatWrite[0].value[0].value)) / statInt)

            statNetworkTx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.transmitted.average')), "", vm, interval)
            if statNetworkTx != False:
                networkTx = (float(sum(statNetworkTx[0].value[0].value) * 8 * 1024) / statInt)
            else:
                networkTx = 0
            statNetworkRx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.received.average')), "", vm, interval)
            if statNetworkRx != False:
                networkRx = (float(sum(statNetworkRx[0].value[0].value) * 8 * 1024) / statInt)            
            else:
                networkRx = 0
            value={"powerState":vm.runtime.powerState,"vm_uptime":uptime,"vm_cpu_usage":cpuUsage,"vm_memory_capacity":memoryCapacity,"vm_memory_usage":memoryUsage,
                    "vm_memory_freePercent":freeMemoryPercentage,"vm_net_if_out":networkTx,"vm_net_if_in":networkRx,
                    "vm_datastore_io_read_bytes":DatastoreRead,"vm_datastore_io_write_bytes":DatastoreWrite,
                    "vm_datastore_io_read_numbers":DatastoreIoRead,"vm_datastore_io_write_numbers":DatastoreIoWrite,
                    "vm_datastore_io_read_latency":DatastoreLatRead,"vm_datastore_io_write_latency":DatastoreLatWrite}
            self._add_data("vm",vm.name,value)            
        except Exception as error:
            _LOGGER.error( "Unable to access information for host: ", vm.name)
            _LOGGER.error( error)
            pass



    def run(self,now):
        self.ts = int(time.time())

        try:
            content = self.si.RetrieveContent()
            vchtime = self.si.CurrentTime()

            perf_dict = {}
            perfList = content.perfManager.perfCounter
            for counter in perfList:
                counter_full = "{}.{}.{}".format(counter.groupInfo.key, counter.nameInfo.key, counter.rollupType)
                perf_dict[counter_full] = counter.key   
            for datacenter in content.rootFolder.childEntity:
                datacenter_name = datacenter.name
                datastores = datacenter.datastore
                for ds in datastores:
                    if (ds.name in self._datastore_names) or (len(self._datastore_names) == 0):
                        self._DatastoreInformation(ds,datacenter_name)

                if hasattr(datacenter.hostFolder, 'childEntity'):
                    hostFolder = datacenter.hostFolder
                    computeResourceList = []
                    computeResourceList = self._getComputeResource(hostFolder,computeResourceList)
                    for computeResource in computeResourceList:
                        self._ComputeResourceInformation(computeResource,datacenter_name,content,perf_dict,vchtime,self._interval)
            
            obj = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
            for vm in obj.view:
                if (vm.name in self._vm_names) or (len(self._vm_names) == 0):
                    tags = "vm=" + vm.name
                    if vm.runtime.powerState == "poweredOn":
                        self._VmInfo(vm, content, vchtime, self._interval, perf_dict, tags)
                    else:
                        value={"powerState":vm.runtime.powerState}
                        self._add_data("vm",vm.name,value)            

        except vmodl.MethodFault as error:
            _LOGGER.error( "Connect Vcenter Error : " + error.msg)
            return False, error.msg
        return True, "ok" 
