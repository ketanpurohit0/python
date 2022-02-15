from collections import deque


def solution(T: Tree):
    # write your code in Python 3.6
    def max_turns(stack: deque, node: Tree):
        # if node is not None:
        #     print (node.x, stack)

        if (node is None):
            return (max(0, len(stack) - 2))
        else:
            left_stack = stack.copy()
            right_stack = stack.copy()

            if (len(left_stack) and left_stack[-1] == 'L'):
                left_stack.pop()
            left_stack.append('L')

            if (len(right_stack) and right_stack[-1] == 'R'):
                right_stack.pop()
            right_stack.append('R')

            return max(max_turns(left_stack, node.l), max_turns(right_stack, node.r))

    d = deque([])
    return max_turns(d, T)


