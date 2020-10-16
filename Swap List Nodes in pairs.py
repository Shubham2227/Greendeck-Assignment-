class Solution:
	# @param A : head node of linked list
	# @return the head node in the linked list
	def swapPairs(self, A):
	    head = A.next if A and A.next else A
	    
	    while A and A.next:
	        thrd = A.next.next                    
	        forth = thrd.next if thrd and thrd.next else thrd  
	        A.next.next, A.next, A = A, forth, thrd
	        
	    return headSwap List Nodes in pairs
