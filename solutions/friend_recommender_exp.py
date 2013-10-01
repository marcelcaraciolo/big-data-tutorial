#-*-coding: utf-8 -*-

'''

    This module represents the recommender system for recommending
    new friends based on 'mutual friends'.


'''
__author__ = 'Marcel Caraciolo <caraciol@gmail.com>'

from mrjob.job import MRJob

TOP_N = 5


class FriendsRecommender(MRJob):

    def steps(self):
        return [self.mr(self.map_input, self.count_number_of_friends),
                self.mr(self.count_max_of_mutual_friends,
                    self.top_recommendations)]

    def map_input(self, key, line):
        '''
        Split input to obtain the pair of friends

        Input (source -> {“friend1”, “friend2”, “friend3”}):
            marcel,jonas,maria,jose,amanda

        Output ({[source, friend1], (-1,source)};
                {[friend1, friend2],(1,source)};):

            ["jonas", "marcel"]    -1,'marcel'
            ["jonas", "maria"]      1,'marcel',
            ["jonas", "jose"]       1,'marcel'
            ["amanda", "jonas"]     1,'marcel'
            ["marcel", "maria"]    -1,'marcel'
            ["jose", "maria"]       1, 'marcel'
            ["amanda", "maria"]     1, 'marcel'
            ["jose", "marcel"]      -1, 'marcel'
            ["amanda", "jose"]      1, 'marcel'
            ["amanda", "marcel"]    -1, 'marcel'

        '''
        input = line.split(',')
        user_id, item_ids = input[0], input[1:]
        for i in range(len(item_ids)):
            f1 = item_ids[i]
            if user_id < f1:
                yield (user_id, f1), (-1, user_id)
            else:
                yield (f1, user_id), (-1, user_id)

            for j in range(i + 1, len(item_ids)):
                f2 = item_ids[j]
                if f1 < f2:
                    yield (f1, f2), (1, user_id)
                else:
                    yield (f2, f1), (1, user_id)

    def count_number_of_friends(self, key, values):
        '''
        Count the number of mutual friends.

        Input ({[friend1, friend2], [(-1,source), (-1,source)]};
               {[friend1, friend2],[(1,source), (1,source)]};):

          ["jonas", "marcel"]    -1,'marcel'
            ["jonas", "maria"]      1,'marcel',
            ["jonas", "jose"]       1,'marcel'
            ["amanda", "jonas"]     1,'marcel'
            ["marcel", "maria"]    -1,'marcel'
            ["jose", "maria"]       1, 'marcel'
            ["amanda", "maria"]     1, 'marcel'
            ["jose", "marcel"]      -1, 'marcel'
            ["amanda", "jose"]      1, 'marcel'
            ["amanda", "marcel"]    -1, 'marcel'


        Output ({[friend1, friend2],
                (numberOfMutualFriends,[mutual1, mutual2, ... mutualn]}):

            ["fabio", "marcel"]     (1, ['jonas'])
            ["fabiola", "marcel"]   (1, ['maria'])
            ["marcel", "patricia"]  (1, ['amanda'])
            ["marcel", "paula"]     (1, ['amanda'])
            ["carol", "marcel"]     (2, ['maria','jose'])

        '''
        f1, f2 = key
        mutual_friends_count = 0
        explanations = []
        for value, user_id in values:
            if value == -1:
                return
            mutual_friends_count += value
            explanations.append(user_id)

        yield (f1, f2), (mutual_friends_count, explanations)

    def count_max_of_mutual_friends(self, key, value):
        '''
        Prepare the dataset to yield the source and
        get the top suggestions.

        Input ({[friend1, friend2],
                (numberOfMutualFriends,[mutual1, mutual2, ... mutualn]}):

            ["fabio", "marcel"]     (1, ['jonas'])
            ["fabiola", "marcel"]   (1, ['maria'])
            ["marcel", "patricia"]  (1, ['amanda'])
            ["marcel", "paula"]     (1, ['amanda'])
            ["carol", "marcel"]     (2, ['maria','jose'])

        Output ({friend1,  [numberOfMutualFriends, friend2,
                                [mutual1, mutual2, ... mutualn]]},
                {friend2,  [numberOfMutualFriends, friend1,
                                [mutual1, mutual2, ... mutualn]]}):

            "fabio", [1,"marcel",['jonas']]
            "marcel", [1,"fabio",['jonas']]
            "fabiola", [1,"marcel",['maria']]
            "marcel", [1,"fabiola",['maria']]
            "marcel", [1,"patricia",['amanda']]
            "patricia", [1,"marcel",['amanda']]
            "marcel", [1,"paula",['amanda']]
            "paula", [1,"marcel",['amanda']]
            "marcel", [2,"carol",['maria','jose']]
            "carol", [2,"marcel",['maria','jose']]

        '''
        f1, f2 = key
        value, explanations = value
        yield f1, (int(value), f2, explanations)
        yield f2, (int(value), f1, explanations)

    def top_recommendations(self, key, values):
        '''
        Get the TOP N recommendations for user.

        Input ({friend1,  [(numberOfMutualFriends, friend, explanations),
                           (numberOfMutualFriends2, friend, explanations)]}):

            "fabio", [1,"marcel",['jonas']]
            "marcel", [[2,"carol",['maria','jose']]
                    , [1,"fabio",['jonas']], [1,"fabiola",['maria']],
                      [1,"patricia",['amanda']],
                        [1,"paula",['amanda']]
            "fabiola", [1,"marcel",['maria']]
            "patricia", [1,"marcel",['amanda']]
            "paula", [[1,"marcel"]]
            "carol", [[2,"marcel"]]

        Output ({friend1,  [(numberOfMutualFriends, friend),
                           (numberOfMutualFriends2, friend)]}):

            Ex: Get the top 3 suggestions.

            "fabio", [[1,"marcel"]]
            "marcel", [[2,"carol"], [1,"fabio"], [1,"fabiola"]]
            "fabiola", [[1,"marcel"]]
            "patricia", [[1,"marcel"]]
            "paula", [1,"marcel",['amanda']]
            "carol", [2,"marcel",['maria','jose']]
        '''
        recommendations = []
        for score, item, explanations in values:
            recommendations.append((item, score, explanations))

        yield key, sorted(recommendations, key=lambda k: -k[1])[:TOP_N]


if __name__ == '__main__':
    FriendsRecommender.run()
