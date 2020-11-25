from flask import request


class requestHandler:
    # workflow ID is formed in following manner
    # <workflow name>#<company name>

    def __init__(self, selfID):
        # selfID : "1" data loader
        #           "2" logistic regression
        #           "3" SVM
        #           "4" analytic
        self.ID = selfID
        self.workflowmap = {}

    def parseReq(self, body, reqType):

        if reqType == "nwf":

            name = body["client_name"]
            workflowName = body["workflow"]
            seq = body["workflow_specification"]
            IPs = body["ips"]
            self._newWorkFlow(workflowName+"#"+name, seq, IPs)

            return (workflowName+"#"+name, None, None)

        elif reqType == "pred":

            name = body["client_name"]
            workflowName = body["workflow"]
            data = body["data"]
            analytics = body["analytics"]

            return (workflowName+"#"+name, data, analytics)

        elif reqType == "fwf":
            split = body.split("#")
            name = split[1]
            workflowName = split[0]
            # seq = body["workflow_specification"]

            # return (workflowName+"#"+name, data, analytics)
            return (workflowName + "#" + name, None, None)

    def _newWorkFlow(self, workFlowID, sequence, IPs):
        '''
        IPs = {
            "1":<IP>,
            "2":<IP>,
            "3":<IP>,
            "4":<IP>
        }
        '''
        print("message : starting new workflow")
        idx = 0
        for i in range(len(sequence)):
            if self.ID in sequence[i]:
                idx = i

        if workFlowID not in self.workflowmap:
            self.workflowmap[workFlowID] = []
           # if workFlowID in self.workflowmap:
        print("message : starting new workflow")
        if i < len(sequence)-1:
            # next will be the next index
            for element in sequence[idx+1]:
                if(element == "1"):
                    self.workflowmap[workFlowID].append(IPs[element
                                                            ]+"/dataflow_append")
                if(element == "2"):
                    self.workflowmap[workFlowID].append(IPs[element
                                                            ]+"/lgr/predict")
        else:
            # last component in workflow
            print("message : append analytics")
            self.workflowmap[workFlowID].append(
                IPs["4"]+"/put_result")

    def getNextAddress(self, key):

        if key not in self.workflowmap.keys():
            return "error"

        return self.workflowmap[key]

    def appendIP(self, body):
        print("message : " + str(body))
        name = body["client_name"]
        workflowName = body["workflow"]
        seq = body["workflow_specification"]
        IPs = body["ips"]
        workFlowID = workflowName+"#"+name
        idx = 0
        for i in range(len(seq)):
            if self.ID in seq[i]:
                idx = i
        if workFlowID not in self.workflowmap:
            self.workflowmap[workFlowID] = []
        # if workFlowID in self.workflowmap:
        if i < len(seq)-1:
            # next will be the next index
            for element in seq[idx+1]:
                if(element == "1"):
                    self.workflowmap[workFlowID].append(IPs[element
                                                            ]+"/dataflow_append")
                if(element == "2"):
                    self.workflowmap[workFlowID].append(IPs[element
                                                            ]+"/lgr/predict")
        else:
            # last component in workflow
            self.workflowmap[workFlowID].append(
                IPs["4"]+"/put_result")
        return workflowName+"#"+name

    def getNextRequest(self, workFlowID):
        pass
