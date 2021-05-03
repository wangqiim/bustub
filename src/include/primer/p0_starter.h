//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include "common/logger.h"

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) : rows(r), cols(c), linear(new T[r * c]) {}

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[](linear); }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      data_[i] = new T[c];
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return this->rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return this->cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override { return data_[i][j]; }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override { data_[i][j] = val; }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    int r = GetRows();
    int c = GetColumns();
    for (int i = 0; i < r; i++) {
      for (int j = 0; j < c; j++) {
        data_[i][j] = arr[i * c + j];
      }
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override {
    int r = GetRows();
    for (int i = 0; i < r; i++) {
      delete[] data_[i];
    }
    delete[] data_;
  }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    std::unique_ptr<RowMatrix<T>> ans;
    if (mat1 == nullptr || mat2 == nullptr) {
      return ans;
    }
    int r1 = mat1->GetRows();
    int r2 = mat2->GetRows();
    int c1 = mat1->GetColumns();
    int c2 = mat2->GetColumns();
    if (r1 != r2 || c1 != c2) {
      return ans;
    }
    T *linear = new T[r1 * c1];
    ans.reset(new RowMatrix<T>(r1, c1));
    for (int i = 0; i < r1; i++) {
      for (int j = 0; j < c1; j++) {
        linear[i * c1 + j] = mat1->GetElem(i, j) + mat2->GetElem(i, j);
      }
    }
    ans->MatImport(linear);
    delete[](linear);
    return ans;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    std::unique_ptr<RowMatrix<T>> ans;
    if (mat1 == nullptr || mat2 == nullptr) {
      return ans;
    }
    int r1 = mat1->GetRows();
    int r2 = mat2->GetRows();
    int c1 = mat1->GetColumns();
    int c2 = mat2->GetColumns();
    if (c1 != r2) {
      return ans;
    }
    T *linear = new T[r1 * c2];
    ans.reset(new RowMatrix<T>(r1, c2));
    for (int i = 0; i < r1; i++) {
      for (int j = 0; j < c2; j++) {
        int sum = 0;
        for (int k = 0; k < c1; k++) {
          sum += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        linear[i * c2 + j] = sum;
      }
    }
    ans->MatImport(linear);
    delete[](linear);
    return ans;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    return AddMatrices(std::move(MultiplyMatrices(std::move(matA), std::move(matB))), std::move(matC));
  }
};
}  // namespace bustub
