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

            name = body["client_name"]
            workflowName = body["workflow"]
            seq = body["workflow_specification"]

            return (workflowName+"#"+name, data, analytics)

    def _newWorkFlow(self, workFlowID, sequence, IPs):
        '''
        IPs = {
            "1":<IP>,
            "2":<IP>,
            "3":<IP>,
            "4":<IP>
        }
        '''

        idx = 0
        for i in range(len(sequence)):
            if self.ID in sequence[i]:
                idx = i

        if workFlowID not in self.workflowmap:
            if i < len(sequence):
                # next will be the next index
                self.workflowmap[workFlowID] = IPs[sequence[idx+1]]
            else:
                # last component in workflow
                self.workflowmap[workFlowID] = IPs["4"]

    def getNextAddress(self, key):

        if key not in self.workflowmap.keys():
            return "error"

        return self.workflowmap[key]

    def appendIP(self, body):
        name = body["client_name"]
        workflowName = body["workflow"]
        seq = body["workflow_specification"]
        IPs = body["ips"]
        workFlowID = workflowName+"#"+name
        idx = 0
        for i in range(len(seq)):
            if self.ID in seq[i]:
                idx = i

        if workFlowID in self.workflowmap:
            if i < len(seq):
                # next will be the next index
                self.workflowmap[workFlowID] = IPs[seq[idx+1]]
            else:
                # last component in workflow
                self.workflowmap[workFlowID] = IPs["4"]
        return workflowName+"#"+name

    def getNextRequest(self, workFlowID):
        pass
