from typing import List

class Solution:

    def encode(self, strs: List[str]) -> str:
        encoded = ""
        for s in strs:
            encoded += str(len(s)) + "#" + s
        print(encoded)
        return encoded

    def decode(self, s: str) -> List[str]:
        print("working")
        res = []
        i = 0
        while i < len(s):
            j = i
            while s[j] != '#':
                j+=1 
                length = int(s[i:j])
                word = s[j+1: j+1+length]
                res.append(word)
                i = j + 1 + length
        return res
    
sol = Solution()
encoded = sol.encode(["hello","word","priyanka"])
print(encoded)         # "5#hello5#world"

decoded = sol.decode(encoded)
print(decoded) 