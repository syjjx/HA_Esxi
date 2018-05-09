import atexit
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnectNoSSL, Disconnect
import sys
import copy
import requests
import time
import json
import config
from datetime import timedelta



class Hello_Esxi():

    def __init__(self,vchost,username,password,prot=443):
        self._vchost=vchost
        self._username=username
        self._password=password
        self._port=port
        self._payload=[]
        self._interval=60

    def hello_vcenter(self):
        try:
            self.si = SmartConnectNoSSL(
                host=self._vchost,
                user=self._username,
                pwd=self._password,
                port=self._port)
            atexit.register(Disconnect, self.si)
            return True, "ok"
        except vmodl.MethodFault as error:
            return False, error.msg
        except Exception as e:
            return False, str(e)

    @property
    def vcenter_status(self):
        return self.hello_vcenter()

    @property
    def vcenter_info(self):
        return json.dumps(self._payload,indent=4)

    def _add_data(self,metric,value,conterType,tags):
        data = {"endpoint":'S_Vcenter   ',"metric":metric,"timestamp":self.ts,"step":self._interval,"value":value,"counterType":conterType,"tags":tags}
        self._payload.append(copy.copy(data))

    def _DatastoreInformation(self,datastore,datacenter_name):
        try:
            summary = datastore.summary
            name = summary.name
            TYPE = summary.type

            tags = "datacenter=" + datacenter_name + ",datastore=" + name + ",type=" + TYPE

            capacity = summary.capacity
            self._add_data("datastore.capacity",capacity,"GAUGE",tags)

            freeSpace = summary.freeSpace
            self._add_data("datastore.free",freeSpace,"GAUGE",tags)

            freeSpacePercentage = (float(freeSpace) / capacity) * 100
            self._add_data("datastore.freePercent",freeSpacePercentage,"GAUGE",tags)
            
        except Exception as error:
            print( "Unable to access summary for datastore: ", datastore.name)
            print( error)
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
                if (host.name in config.esxi_names) or (len(config.esxi_names) == 0):
                    self._HostInformation(host,datacenter_name,computeResource_name,content,perf_dict,vchtime,interval)
        except Exception as error:
            print( "Unable to access information for compute resource: ", computeResource.name)
            print( error)
            pass

    def _HostInformation(self,host,datacenter_name,computeResource_name,content,perf_dict,vchtime,interval):
        try:
            statInt = interval/20
            summary = host.summary
            stats = summary.quickStats
            hardware = host.hardware

            tags = "datacenter=" + datacenter_name + ",cluster_name=" + computeResource_name + ",host=" + host.name

            uptime = stats.uptime
            self._add_data("esxi.uptime",uptime,"GAUGE",tags)

            cpuUsage = 100 * 1000 * 1000 * float(stats.overallCpuUsage) / float(hardware.cpuInfo.numCpuCores * hardware.cpuInfo.hz)
            self._add_data("esxi.cpu.usage",cpuUsage,"GAUGE",tags)

            memoryCapacity = hardware.memorySize
            self._add_data("esxi.memory.capacity",memoryCapacity,"GAUGE",tags)

            memoryUsage = stats.overallMemoryUsage * 1024 * 1024
            self._add_data("esxi.memory.usage",memoryUsage,"GAUGE",tags)

            freeMemoryPercentage = 100 - (
                (float(memoryUsage) / memoryCapacity) * 100
            )
            self._add_data("esxi.memory.freePercent",freeMemoryPercentage,"GAUGE",tags)

            statNetworkTx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.transmitted.average')), "", host, interval)     
            networkTx = (float(sum(statNetworkTx[0].value[0].value) * 8 * 1024) / statInt)
            self._add_data("esxi.net.if.out",networkTx,"GAUGE",tags)
            
            statNetworkRx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.received.average')), "", host, interval)
            networkRx = (float(sum(statNetworkRx[0].value[0].value) * 8 * 1024) / statInt)
            self._add_data("esxi.net.if.in",networkRx,"GAUGE",tags)

            self._add_data("esxi.alive",1,"GAUGE",tags)

        except Exception as error:
            print( "Unable to access information for host: ", host.name)
            print( error)
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
            self._add_data("vm.uptime",uptime,"GAUGE",tags)
            
            cpuUsage = 100 * float(stats.overallCpuUsage)/float(summary.runtime.maxCpuUsage)
            self._add_data("vm.cpu.usage",cpuUsage,"GAUGE",tags)
            
            memoryUsage = stats.guestMemoryUsage * 1024 * 1024
            self._add_data("vm.memory.usage",memoryUsage,"GAUGE",tags)
            
            memoryCapacity = summary.runtime.maxMemoryUsage * 1024 * 1024
            self._add_data("vm.memory.capacity",memoryCapacity,"GAUGE",tags)
            
            freeMemoryPercentage = 100 - (
                (float(memoryUsage) / memoryCapacity) * 100
            )
            self._add_data("vm.memory.freePercent",freeMemoryPercentage,"GAUGE",tags)
            
            statDatastoreRead = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.read.average')),"*", vm, interval)
            DatastoreRead = (float(sum(statDatastoreRead[0].value[0].value) * 1024) / statInt)
            self._add_data("vm.datastore.io.read_bytes",DatastoreRead,"GAUGE",tags)
            
            statDatastoreWrite = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.write.average')),"*", vm, interval)
            DatastoreWrite = (float(sum(statDatastoreWrite[0].value[0].value) * 1024) / statInt)
            self._add_data("vm.datastore.io.write_bytes",DatastoreWrite,"GAUGE",tags)
            
            statDatastoreIoRead = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.numberReadAveraged.average')),"*", vm, interval)
            DatastoreIoRead = (float(sum(statDatastoreIoRead[0].value[0].value)) / statInt)
            self._add_data("vm.datastore.io.read_numbers",DatastoreIoRead,"GAUGE",tags)
            
            statDatastoreIoWrite = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.numberWriteAveraged.average')),"*", vm, interval)
            DatastoreIoWrite = (float(sum(statDatastoreIoWrite[0].value[0].value)) / statInt)
            self._add_data("vm.datastore.io.write_numbers",DatastoreIoWrite,"GAUGE",tags)
            
            statDatastoreLatRead = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.totalReadLatency.average')), "*", vm, interval)
            DatastoreLatRead = (float(sum(statDatastoreLatRead[0].value[0].value)) / statInt)
            self._add_data("vm.datastore.io.read_latency",DatastoreLatRead,"GAUGE",tags)

            statDatastoreLatWrite = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'datastore.totalWriteLatency.average')), "*", vm, interval)
            DatastoreLatWrite = (float(sum(statDatastoreLatWrite[0].value[0].value)) / statInt)
            self._add_data("vm.datastore.io.write_latency",DatastoreLatWrite,"GAUGE",tags)

            statNetworkTx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.transmitted.average')), "", vm, interval)
            if statNetworkTx != False:
                networkTx = (float(sum(statNetworkTx[0].value[0].value) * 8 * 1024) / statInt)
                self._add_data("vm.net.if.out",networkTx,"GAUGE",tags)
            statNetworkRx = self._BuildQuery(content, vchtime, (self._perf_id(perf_dict, 'net.received.average')), "", vm, interval)
            if statNetworkRx != False:
                networkRx = (float(sum(statNetworkRx[0].value[0].value) * 8 * 1024) / statInt)        
                self._add_data("vm.net.if.in",networkRx,"GAUGE",tags)      
            
        except Exception as error:
            print( "Unable to access information for host: ", vm.name)
            print( error)
            pass



    def run(self,interval):
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
                    if (ds.name in config.datastore_names) or (len(config.datastore_names) == 0):
                        self._DatastoreInformation(ds,datacenter_name)
                # print(json.dumps(self._payload,indent=4))
                if hasattr(datacenter.hostFolder, 'childEntity'):
                    hostFolder = datacenter.hostFolder
                    computeResourceList = []
                    computeResourceList = self._getComputeResource(hostFolder,computeResourceList)
                    for computeResource in computeResourceList:
                        self._ComputeResourceInformation(computeResource,datacenter_name,content,perf_dict,vchtime,interval)
            
            if config.vm_enable == True:
                obj = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
                for vm in obj.view:
                    if (vm.name in config.vm_names) or (len(config.vm_names) == 0):
                        tags = "vm=" + vm.name
                        if vm.runtime.powerState == "poweredOn":
                            self._VmInfo(vm, content, vchtime, interval, perf_dict, tags)
                            self._add_data("vm.power",1,"GAUGE",tags)
                        else:
                            self._add_data("vm.power",0,"GAUGE",tags)               

        except vmodl.MethodFault as error:
            print( "Caught vmodl fault : " + error.msg)
            return False, error.msg
        return True, "ok"



if __name__ == "__main__":
    host = config.host
    user = config.user
    pwd = config.pwd
    port = config.port

    client=Hello_Esxi(host,user,pwd,port)
    success,msg = client.vcenter_status
    if success == True:
        while True:
            client.run(60)
            print(client.vcenter_info)
            time.sleep(60)


    
    # endpoint = config.endpoint
    # push_api = config.push_api
    # interval = config.interval

    # ts = int(time.time())
    # payload = []

    # success, msg = hello_vcenter(host,user,pwd,port)
    # if success == False:
    #     print( msg)
    #     add_data("vcenter.alive",0,"GAUGE","")
    #     # print( json.dumps(payload,indent=4))
    #     # r= requests.post(push_api, data=json.dumps(payload))

    #     sys.exit(1)
    #     add_data("vcenter.alive",1,"GAUGE","")
    
    # run(host,user,pwd,port,interval)
    # print( json.dumps(payload,indent=4))
    # # r = requests.post(push_api, data=json.dumps(payload))
