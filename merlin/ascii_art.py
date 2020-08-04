###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

"""
Holds ascii art strings.
"""

merlin_name = """ 
                                                                            
     *****   **    **                          ***                          
  ******  ***** *****                           ***      *                  
 **   *  *  ***** *****                          **     ***                 
*    *  *   * **  * **                           **      *                  
    *  *    *     *               ***  ****      **                         
   ** **    *     *        ***     **** **** *   **    ***     ***  ****    
   ** **    *     *       * ***     **   ****    **     ***     **** **** * 
   ** **    *     *      *   ***    **           **      **      **   ****  
   ** **    *     *     **    ***   **           **      **      **    **   
   ** **    *     **    ********    **           **      **      **    **   
   *  **    *     **    *******     **           **      **      **    **   
      *     *      **   **          **           **      **      **    **   
  ****      *      **   ****    *   ***          **      **      **    **   
 *  *****           **   *******     ***         *** *   *** *   ***   ***  
*     **                  *****                   ***     ***     ***   *** 
*                                                                           
 **                                                                         
                                                                            
"""

merlin_hat = """ 
                
           ..*O****..*...              
         ...*...*...******...             
        ...*.....**.*..*..***O          
       ...*.......*O.......**.         
      ...O         .*.....*...         
    ...            ...*..*....         
   ...            .....*......         
  ..             .....*.*.....         
 .              ....**...*.....         
               ....*.....*....         
               ...*........*...         
              ..**.........*..         
             ..*.............*.         
            .**...............*O        
            *................***        
          O...............*..*.*.       
          .*.*.........*....*..*..      
         ......*....*......*...*....    
        ....*....*........*....*.....   
       .......*....*.....*.....*......  
      .....*.*.......*..*......*....... 
     ...*..............*.......*......  
    .*........*.......*..*.....*....    
   O.................*.....*...*...     
      .........*....*........*.*.       
            .......*......     O        
                 O                       
 
"""

merlin_name_small = """ 
                                  

                                  
  __  __           _ _       
 |  \/  |         | (_)      
 | \  / | ___ _ __| |_ _ __  
 | |\/| |/ _ \ '__| | | '_ \ 
 | |  | |  __/ |  | | | | | |
 |_|  |_|\___|_|  |_|_|_| |_|
                                   
 Machine Learning for HPC Workflows                                 

"""

merlin_hat_small = """ 
              
       *      
   *~~~~~     
  *~~*~~~*    
 /   ~~~~~    
     ~~~~~    
    ~~~~~*    
   *~~~~~~~   
  ~~~~~~~~~~  
 *~~~~~~~~~~~ 
   ~~~*~~~*   
              
"""


def _make_banner():

    name_lines = merlin_name_small.split("\n")
    hat_lines = merlin_hat_small.split("\n")

    banner = ""
    for hat_line, name_line in zip(hat_lines, name_lines):
        banner = banner + hat_line + name_line + "\n"

    return banner


banner_small = _make_banner()
