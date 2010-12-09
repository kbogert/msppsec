


class nullPPSEC:
    
    def encrypt(self, plainText):
        return plainText
    
    def decrypt(self, encText):
        return encText
    
    def authenticateIncoming(self, sourceID, graphID, socket):
        return True
    
    def authenticateOutgoing(self, socket):
        return True
    
    def directMessageCallback(self, message):
        pass
    
    
