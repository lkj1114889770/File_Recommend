from django.shortcuts import render
from Web.models import getData

# Create your views here.


def home(request):
    number = 10
    genres = ['Action', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Horror', 'Romance', 'Science_Fiction', 'Thriller']
    items = ['title', 'year', 'vote_average']
    indexOfItem = {'title': 0, 'year': 1, 'vote_average': 3}
    topData = []
    context = []
    # tableName = []
    dataType = 'tuple'
    for genre in genres:
        # tableName.append('tb_TOP_' + genre)
        topData.append(list(getData('tb_TOP_' + genre, number, dataType)))
    count = 0
    for data in topData:
        tempDir = {'genre': genres[count], 'data': []}
        count = count + 1
        for i in range(number):
            temp = {}
            for item in items:
                temp[item] = data[i][indexOfItem[item]]
            tempDir['data'].append(temp)
        tempDir['image1'] = (data[0][0] + ' (' + data[0][1] + ')').replace(':', '')
        tempDir['image2'] = (data[1][0] + ' (' + data[1][1] + ')').replace(':', '')
        tempDir['image3'] = (data[2][0] + ' (' + data[2][1] + ')').replace(':', '')
        context.append(tempDir)
    return render(request, 'index.html', {'home': context})


def search(request):
    if request.POST:
        var = request.POST['var']
    return render(request, 'movies.html')