#-*-coding: utf-8 -*-

'''

    This module represents the recommender system for recommending
    new friends based on 'mutual friends'.


'''
__author__ = 'Marcel Caraciolo <caraciol@gmail.com>'

from mrjob.job import MRJob
import sys

TOP_N = 10


class FriendsRecommender(MRJob):

    def steps(self):
        return [self.mr(self.map_input, self.count_number_of_friends),
                self.mr(self.count_max_of_mutual_friends,
                    self.top_recommendations)]

    def map_input(self, key, line):
        '''
        Compute a cartesian product using nested loops
        for each friend in connection_list

        Input (source -> {“friend1”, “friend2”, “friend3”}):
            marcel,jonas,maria,jose,amanda

        Output {[source, friend1], -1};
               {[friend1, friend2], 1};):

            ["jonas", "marcel"] -1
            ["jonas", "maria"]  1
            ["jonas", "jose"]   1
            ["amanda", "jonas"] 1
            ["marcel", "maria"] -1
            ["jose", "maria"]   1
            ["amanda", "maria"] 1
            ["jose", "marcel"]  -1
            ["amanda", "jose"]  1
            ["amanda", "marcel"]    -1

        '''
        input = line.split(',')
        user_id, item_ids = input[0], input[1:]
        for i in range(len(item_ids)):
            f1 = item_ids[i]
            if user_id < f1:
                yield (user_id, f1), -1
            else:
                yield (f1, user_id), -1

            for j in range(i + 1, len(item_ids)):
                f2 = item_ids[j]
                if f1 < f2:
                    yield (f1, f2), 1
                else:
                    yield (f2, f1), 1

    def count_number_of_friends(self, key, values):
        '''
        Count the number of mutual friends.

        Input ({[friend1, friend2], [-1, -1]};
               {[friend1, friend2],[1, 1]};):

            ["jonas", "marcel"] -1
            ["marcel", "maria"] -1
            ["jose", "marcel"]  -1
            ["amanda", "marcel"]    -1
            ["fabio", "marcel"] 1
            ["fabiola", "marcel"]   1
            ["carol", "marcel"] 1
            ["carol", "marcel"] 1


        Output ({[friend1, friend2], numberOfMutualFriends}):

            ["fabio", "marcel"] 1
            ["fabiola", "marcel"]   1
            ["marcel", "patricia"]  1
            ["marcel", "paula"] 1
            ["carol", "marcel"] 2

        '''

        f1, f2 = key
        mutual_friends_count = 0
        for value in values:
            if value == -1:
                return
            mutual_friends_count += value

        yield (f1, f2), mutual_friends_count
        #yield '%010s' % (sys.maxint - int(mutual_friends_count)), \
        #        (f1, f2, int(mutual_friends_count))

    def count_max_of_mutual_friends(self, key, values):

        '''
        Prepare the dataset to yield the source and
        get the top suggestions.

        Input ({[friend1, friend2], numberOfMutualFriends}):

            ["fabio", "marcel"] 1
            ["fabiola", "marcel"]   1
            ["marcel", "patricia"]  1
            ["marcel", "paula"] 1
            ["carol", "marcel"] 2

        Output ({friend1,  [numberOfMutualFriends, friend2]},
                {friend2,  [numberOfMutualFriends, friend1]}):

            "fabio", [1,"marcel"]
            "marcel", [1,"fabio"]
            "fabiola", [1,"marcel"]
            "marcel", [1,"fabiola"]
            "marcel", [1,"patricia"]
            "patricia", [1,"marcel"]
            "marcel", [1,"paula"]
            "paula", [1,"marcel"]
            "marcel", [2,"carol"]
            "carol", [2,"marcel"]

        '''
        f1, f2 = key
        # for score in values:
        yield f1, (f2, int(values))
        yield f2, (f1, int(values))

    def top_recommendations(self, key, values):
        '''
        Get the TOP N recommendations for user.

        Input ({friend1,  [(numberOfMutualFriends, friend),
                           (numberOfMutualFriends2, friend)]}):

            "fabio", [[1,"marcel"]]
            "marcel", [[2,"carol"], [1,"fabio"], [1,"fabiola"], [1,"patricia"],
                        [1,"paula"]]
            "fabiola", [[1,"marcel"]]
            "patricia", [[1,"marcel"]]
            "paula", [[1,"marcel"]]
            "carol", [[2,"marcel"]]

        Output ({friend1,  [(numberOfMutualFriends, friend),
                           (numberOfMutualFriends2, friend)]}):

            Ex: Get the top 3 suggestions.

            "fabio", [[1,"marcel"]]
            "marcel", [[2,"carol"], [1,"fabio"], [1,"fabiola"]]
            "fabiola", [[1,"marcel"]]
            "patricia", [[1,"marcel"]]
            "paula", [[1,"marcel"]]
            "carol", [[2,"marcel"]]
        '''
        recommendations = []
        for idx, (item, score) in enumerate(values):
            recommendations.append((item, score))

        yield key, sorted(recommendations, key=lambda k: -k[1])[:TOP_N]


if __name__ == '__main__':
    FriendsRecommender.run()
